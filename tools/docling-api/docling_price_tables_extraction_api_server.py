#!/usr/bin/env python3
"""
Docling Price Tables Extraction API Server

This script creates a Flask API server focused on extracting price tables
from PDF documents via Docling.
"""

import os
import json
import tempfile
import zipfile
import uuid
from flask import Response
from flask import Flask, request, jsonify
import logging
from werkzeug.utils import secure_filename
from docling.document_converter import DocumentConverter
from price_table_extraction import extract_price_tables_from_text

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# Ensure Flask doesn't escape non-ASCII characters in JSON (e.g. "º", "ª")
app.config['JSON_AS_ASCII'] = False

# Configuration
UPLOAD_FOLDER = tempfile.gettempdir()
ALLOWED_EXTENSIONS = {'pdf', 'zip'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload size


def allowed_file(filename):
    """Check if the file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def json_response(payload, status=200):
    """Return a Response with UTF-8 JSON (no ASCII escaping)."""
    return Response(json.dumps(payload, ensure_ascii=False), status=status, mimetype='application/json')


def export_document_payload(document):
    """Export multiple representations from a Docling document safely."""
    exports = {}
    try:
        exports["text"] = document.export_to_text()
    except Exception:
        exports["text"] = None

    for method_name, export_key in [
        ("export_to_markdown", "markdown"),
        ("export_to_html", "html"),
    ]:
        exporter = getattr(document, method_name, None)
        if exporter:
            try:
                exports[export_key] = exporter()
            except Exception:
                exports[export_key] = None

    return exports


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'docling-price-tables-api',
        'version': '2.38.1'
    })


@app.route('/extract-price-tables', methods=['POST'])
def extract_price_tables():
    """
    Extract price tables from one or multiple uploaded PDF / image files.

    Request format (multipart/form-data):
    • For multiple files  -> field name "files" (use input[name="files"] or similar)
    • For single  file    -> field name "file"  (kept for backward-compatibility)

    Response JSON structure:
    {
        "success": true,
        "results": [
            {
                "fileName": "example.pdf",
                "extracted_tables": { ... }
            },
            ...
        ],
        "errors": [  // optional
            { "fileName": "bad.pdf", "error": "Could not extract any price tables" },
            ...
        ]
    }
    """

    # Gather uploaded files (support both 'files' and legacy 'file')
    uploaded_files = []
    if 'files' in request.files:
        uploaded_files = request.files.getlist('files')
    elif 'file' in request.files:
        uploaded_files = [request.files['file']]

    if not uploaded_files:
        return jsonify({'success': False, 'error': 'No file(s) provided in the request.'}), 400

    results = []
    errors = []

    for file in uploaded_files:
        if file.filename == '':
            errors.append({'fileName': '', 'error': 'Empty filename.'})
            continue

        if not allowed_file(file.filename):
            errors.append({'fileName': file.filename, 'error': 'File type not allowed.'})
            continue

        filename = secure_filename(file.filename)
        temp_filename = f"{uuid.uuid4()}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], temp_filename)

        # Persist temporarily
        file.save(filepath)

        # If the upload is a ZIP, iterate over contained PDFs
        if filename.lower().endswith('.zip'):
            try:
                with zipfile.ZipFile(filepath, 'r') as zf:
                    pdf_names = [n for n in zf.namelist() if n.lower().endswith('.pdf')]
                    if not pdf_names:
                        errors.append({'fileName': filename, 'error': 'ZIP does not contain any PDF files.'})
                    for inner_name in pdf_names:
                        inner_filename = secure_filename(os.path.basename(inner_name))
                        inner_temp = os.path.join(app.config['UPLOAD_FOLDER'], f"{uuid.uuid4()}_{inner_filename}")
                        # Extract to temp file
                        with open(inner_temp, 'wb') as f_out:
                            f_out.write(zf.read(inner_name))
                        try:
                            converter = DocumentConverter()
                            result = converter.convert(inner_temp)
                            text = result.document.export_to_text()
                            tables = extract_price_tables_from_text(text)
                            # Validate new schema: consider success if any tarifas list is non-empty
                            try:
                                pot = tables.get('termino_de_potencia', {}).get('tabla_precio_potencia', {}).get('tarifas', [])
                                base = tables.get('termino_de_energia', {}).get('tabla_precio_clasica_base', {}).get('tarifas', [])
                                unica = tables.get('termino_de_energia', {}).get('tabla_precio_clasica_unica', {}).get('tarifas', [])
                                has_any = any([pot, base, unica]) and (
                                    (isinstance(pot, list) and len(pot) > 0) or
                                    (isinstance(base, list) and len(base) > 0) or
                                    (isinstance(unica, list) and len(unica) > 0)
                                )
                            except Exception:
                                has_any = False
                            if not tables or not has_any:
                                errors.append({'fileName': inner_filename, 'error': 'Could not extract any price tables from the document.'})
                            else:
                                results.append({'fileName': inner_filename, 'extracted_tables': tables})
                        except Exception as e:
                            app.logger.error(f"Error processing {inner_filename}: {e}")
                            errors.append({'fileName': inner_filename, 'error': f'Processing error: {str(e)}'})
                        finally:
                            try:
                                os.remove(inner_temp)
                            except Exception:
                                pass
            finally:
                # remove uploaded zip
                try:
                    os.remove(filepath)
                except Exception:
                    pass
            # Skip normal PDF processing for ZIP file
            continue

        try:
            # OCR the document (regular single PDF case)
            converter = DocumentConverter()
            result = converter.convert(filepath)
            text = result.document.export_to_text()
            app.logger.debug(f"--- OCR Extracted Text ({filename}) ---\n{text}\n--------------------------")

            # Extract tables
            tables = extract_price_tables_from_text(text)

            # Validate new schema: consider success if any tarifas list is non-empty
            try:
                pot = tables.get('termino_de_potencia', {}).get('tabla_precio_potencia', {}).get('tarifas', [])
                base = tables.get('termino_de_energia', {}).get('tabla_precio_clasica_base', {}).get('tarifas', [])
                unica = tables.get('termino_de_energia', {}).get('tabla_precio_clasica_unica', {}).get('tarifas', [])
                has_any = any([pot, base, unica]) and (
                    (isinstance(pot, list) and len(pot) > 0) or
                    (isinstance(base, list) and len(base) > 0) or
                    (isinstance(unica, list) and len(unica) > 0)
                )
            except Exception:
                has_any = False

            if not tables or not has_any:
                errors.append({'fileName': filename, 'error': 'Could not extract any price tables from the document.'})
            else:
                results.append({'fileName': filename, 'extracted_tables': tables})
        except Exception as e:
            app.logger.error(f"Error processing {filename}: {e}")
            errors.append({'fileName': filename, 'error': f'Processing error: {str(e)}'})
        finally:
            # Always attempt to delete the temporary file
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
            except Exception as cleanup_e:
                app.logger.warning(f"Failed to remove temp file {filepath}: {cleanup_e}")

    # Build final response
    response_payload = {
        'success': bool(results),
        'results': results
    }
    if errors:
        response_payload['errors'] = errors

    status_code = 200 if results else 400
    return json_response(response_payload, status=status_code)


@app.route('/extract-generic', methods=['POST'])
def extract_generic():
    """
    Extract raw Docling exports plus parsed price tables for debugging new formats.

    Request format (multipart/form-data):
    • For multiple files  -> field name "files"
    • For single  file    -> field name "file"
    """
    uploaded_files = []
    if 'files' in request.files:
        uploaded_files = request.files.getlist('files')
    elif 'file' in request.files:
        uploaded_files = [request.files['file']]

    if not uploaded_files:
        return jsonify({'success': False, 'error': 'No file(s) provided in the request.'}), 400

    results = []
    errors = []

    def process_pdf(path, name):
        converter = DocumentConverter()
        result = converter.convert(path)
        exports = export_document_payload(result.document)
        text = exports.get("text") or ""
        tables = extract_price_tables_from_text(text) if text else {}
        return {
            'fileName': name,
            'extracted_tables': tables,
            'exports': exports,
        }

    for file in uploaded_files:
        if file.filename == '':
            errors.append({'fileName': '', 'error': 'Empty filename.'})
            continue

        if not allowed_file(file.filename):
            errors.append({'fileName': file.filename, 'error': 'File type not allowed.'})
            continue

        filename = secure_filename(file.filename)
        temp_filename = f"{uuid.uuid4()}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], temp_filename)
        file.save(filepath)

        if filename.lower().endswith('.zip'):
            try:
                with zipfile.ZipFile(filepath, 'r') as zf:
                    pdf_names = [n for n in zf.namelist() if n.lower().endswith('.pdf')]
                    if not pdf_names:
                        errors.append({'fileName': filename, 'error': 'ZIP does not contain any PDF files.'})
                    for inner_name in pdf_names:
                        inner_filename = secure_filename(os.path.basename(inner_name))
                        inner_temp = os.path.join(app.config['UPLOAD_FOLDER'], f"{uuid.uuid4()}_{inner_filename}")
                        with open(inner_temp, 'wb') as f_out:
                            f_out.write(zf.read(inner_name))
                        try:
                            results.append(process_pdf(inner_temp, inner_filename))
                        except Exception as e:
                            app.logger.error(f"Error processing {inner_filename}: {e}")
                            errors.append({'fileName': inner_filename, 'error': f'Processing error: {str(e)}'})
                        finally:
                            try:
                                os.remove(inner_temp)
                            except Exception:
                                pass
            finally:
                try:
                    os.remove(filepath)
                except Exception:
                    pass
            continue

        try:
            results.append(process_pdf(filepath, filename))
        except Exception as e:
            app.logger.error(f"Error processing {filename}: {e}")
            errors.append({'fileName': filename, 'error': f'Processing error: {str(e)}'})
        finally:
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
            except Exception as cleanup_e:
                app.logger.warning(f"Failed to remove temp file {filepath}: {cleanup_e}")

    response_payload = {
        'success': bool(results),
        'results': results,
    }
    if errors:
        response_payload['errors'] = errors

    status_code = 200 if results else 400
    return json_response(response_payload, status=status_code)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5010))
    app.run(host='0.0.0.0', port=port, debug=False)
