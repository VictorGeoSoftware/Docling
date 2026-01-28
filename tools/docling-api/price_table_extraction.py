import re
import logging
import unicodedata
from typing import Optional

# Set up logging
logger = logging.getLogger(__name__)


def extract_price_tables_from_text(text):
    """
    Extracts three different price tables from a given text block using a columnar format.
    Args:
        text (str): The OCR text from the document image.
    Returns:
        dict: A dictionary containing the extracted tables as dictionaries of lists (columns).
    """
    # -----------------------------
    # Helpers: normalization & utils
    # -----------------------------
    def strip_accents(s: str) -> str:
        if not isinstance(s, str):
            return s
        # Normalize and remove combining marks (accents)
        nfkd = unicodedata.normalize('NFKD', s)
        return ''.join(ch for ch in nfkd if not unicodedata.combining(ch))

    def normalize_text(s: str) -> str:
        if not isinstance(s, str):
            return s
        s = strip_accents(s).lower()
        # Normalize newlines first (preserve line breaks for row parsing)
        s = s.replace('\r\n', '\n').replace('\r', '\n')
        # Unify some punctuation variants and spaces
        s = s.replace('€ /', '€/').replace('c€ /', 'c€/')
        s = s.replace('k w', 'kw').replace('kwh', 'kwh')
        # Collapse only spaces/tabs, keep newlines
        s = re.sub(r"[ \t]+", " ", s)
        return s

    def clean_and_convert(value):
        if not isinstance(value, str):
            return value
        cleaned_value = value.replace('|', '').strip()
        if cleaned_value in ['—', '-', '']:
            return None
        standardized_value = cleaned_value.replace('.', '').replace(',', '.')
        try:
            return float(standardized_value)
        except (ValueError, TypeError):
            return cleaned_value

    def parse_table_to_columns(orig_text: str,
                               norm_text: str,
                               headers: list[str],
                               start_pattern: re.Pattern,
                               end_pattern: Optional[re.Pattern]):
        logger.debug(f"--- Parsing Table (pattern): {start_pattern.pattern} ---")
        try:
            start_m = start_pattern.search(norm_text)
            if not start_m:
                logger.warning(f"Start marker pattern not found: {start_pattern.pattern}")
                return {}

            start_idx = start_m.start()
            if end_pattern:
                end_m = end_pattern.search(norm_text, start_idx)
            else:
                end_m = None

            # Work on normalized slice for robustness
            table_block = norm_text[start_idx:end_m.start()] if end_m else norm_text[start_idx:]

            lines = table_block.strip().split('\n')
            logger.debug(f"Found {len(lines)} normalized lines for table pattern '{start_pattern.pattern}'.")

            # Normalize headers for matching
            norm_headers = [normalize_text(h) for h in headers]

            # Locate header row; try single-line, then two-line concatenation
            header_row_index = -1
            for i, line in enumerate(lines):
                if all(h in line for h in norm_headers):
                    header_row_index = i
                    logger.debug(f"Header row (1-line) at index {i}: '{line}'")
                    break
            if header_row_index == -1 and len(lines) >= 2:
                for i in range(len(lines) - 1):
                    combined = (lines[i] + ' ' + lines[i + 1]).strip()
                    if all(h in combined for h in norm_headers):
                        header_row_index = i
                        logger.debug(f"Header row (2-lines) starting at {i}: '{combined}'")
                        break

            if header_row_index == -1:
                logger.warning(f"Header row containing {headers} not found for table pattern '{start_pattern.pattern}'")
                return {}

            columns = {header: [] for header in headers}
            data_lines = lines[header_row_index + 1:]

            for i, line in enumerate(data_lines):
                # Prefer '|' as delimiter, else split on 2+ spaces
                parts = [p.strip() for p in (line.split('|') if '|' in line else re.split(r'\s{2,}', line)) if p.strip()]
                logger.debug(f"Line {i}: '{line}' -> Parsed parts: {parts}")

                if not parts or (parts and all(c in '-–—' for c in parts[0])):
                    logger.debug(f"Skipping separator or empty line: {parts}")
                    continue

                for idx, header in enumerate(headers):
                    if idx < len(parts):
                        columns[header].append(clean_and_convert(parts[idx]))
                    else:
                        columns[header].append(None)

            if columns:
                max_len = max((len(v) for v in columns.values()), default=0)
                for header in headers:
                    while len(columns[header]) < max_len:
                        columns[header].append(None)

            logger.debug(f"Final columns for pattern '{start_pattern.pattern}': {columns}")
            return columns
        except Exception as e:
            logger.error(f"An error occurred while parsing table (pattern '{start_pattern.pattern}'): {e}", exc_info=True)
            return {}

    def expand_unica_table(table_data):
        if not table_data or 'P1 - P6' not in table_data:
            return {}
        logger.debug("--- Expanding UNICA Table ---")
        expanded_table = {}
        price_values = table_data.get('P1 - P6', [])
        if 'TARIFA' in table_data:
            expanded_table['TARIFA'] = table_data['TARIFA']
        if 'POTENCIA CONTRATADA' in table_data:
            expanded_table['POTENCIA CONTRATADA'] = table_data['POTENCIA CONTRATADA']
        for i in range(1, 7):
            expanded_table[f'P{i}'] = price_values
        logger.debug(f"Expanded UNICA table: {expanded_table}")
        return expanded_table

    def columns_to_tarifas(columns: dict) -> list:
        """Convert parsed columnar table into list of row dicts with expected keys.
        Expected input keys: 'TARIFA', 'POTENCIA CONTRATADA', 'P1'..'P6' (any may be missing).
        """
        if not columns:
            return []
        # Determine number of rows by the longest column
        max_len = 0
        for v in columns.values():
            try:
                max_len = max(max_len, len(v))
            except TypeError:
                pass
        tarifas = []
        for i in range(max_len):
            row = {
                'tarifa': (columns.get('TARIFA', [None] * max_len)[i]
                           if len(columns.get('TARIFA', [])) > i else None),
                'potencia_contratada': (columns.get('POTENCIA CONTRATADA', [None] * max_len)[i]
                                        if len(columns.get('POTENCIA CONTRATADA', [])) > i else None),
                'P1': (columns.get('P1', [None] * max_len)[i]
                       if len(columns.get('P1', [])) > i else None),
                'P2': (columns.get('P2', [None] * max_len)[i]
                       if len(columns.get('P2', [])) > i else None),
                'P3': (columns.get('P3', [None] * max_len)[i]
                       if len(columns.get('P3', [])) > i else None),
                'P4': (columns.get('P4', [None] * max_len)[i]
                       if len(columns.get('P4', [])) > i else None),
                'P5': (columns.get('P5', [None] * max_len)[i]
                       if len(columns.get('P5', [])) > i else None),
                'P6': (columns.get('P6', [None] * max_len)[i]
                       if len(columns.get('P6', [])) > i else None),
            }
            tarifas.append(row)
        return tarifas

    def find_title_line(orig_text: str, title_rx_norm: re.Pattern) -> Optional[str]:
        """Find the original title line whose normalized form matches title_rx_norm.
        Returns the original line (preserving accents/case) or None.
        """
        if not orig_text:
            return None
        lines = orig_text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
        for line in lines:
            norm_line = normalize_text(line)
            if title_rx_norm.search(norm_line or ''):
                # Clean up table formatting artifacts and duplicates
                cleaned = line.strip().strip('#').strip('|').strip()

                # Remove duplicate content (split by | and take unique parts)
                if '|' in cleaned:
                    parts = [part.strip() for part in cleaned.split('|') if part.strip()]
                    # Find the actual title part (contains the pattern we're looking for)
                    for part in parts:
                        if title_rx_norm.search(normalize_text(part) or ''):
                            cleaned = part.strip()
                            break

                return cleaned if cleaned else None
        return None

    def extract_tariff_name(orig_text: str) -> Optional[str]:
        """Extract tariff name from document header (e.g., 'TARIFA CLÁSICA TE1').
        Returns the tariff name or None if not found.
        """
        if not orig_text:
            return None
        lines = orig_text.replace('\r\n', '\n').replace('\r', '\n').split('\n')

        # Look for tariff patterns in the first 20 lines of the document
        # Use more flexible patterns to handle accents and variations
        tariff_patterns = [
            re.compile(r'tarifa\s+cl[aá]sica\s+te\s*\d+', re.IGNORECASE),
            re.compile(r'tarifa\s+te\s*\d+', re.IGNORECASE),
            re.compile(r'cl[aá]sica\s+te\s*\d+', re.IGNORECASE),
            # Also try without accents (normalized)
            re.compile(r'tarifa\s+clasica\s+te\s*\d+', re.IGNORECASE)
        ]

        for i, line in enumerate(lines[:20]):  # Check first 20 lines
            line_clean = line.strip()
            if not line_clean:
                continue

            # Try both original and normalized versions
            test_lines = [line_clean, normalize_text(line_clean)]

            for test_line in test_lines:
                for pattern in tariff_patterns:
                    match = pattern.search(test_line)
                    if match:
                        # Return the original case version if found in original,
                        # otherwise return the matched part in uppercase
                        if test_line == line_clean:
                            return match.group(0).upper()
                        # Found in normalized, try to extract from original
                        orig_match = re.search(r'tarifa.*te\s*\d+', line_clean, re.IGNORECASE)
                        if orig_match:
                            return orig_match.group(0).upper()
                        return match.group(0).upper()

        return None

    potencia_headers = ['TARIFA', 'POTENCIA CONTRATADA', 'P1', 'P2', 'P3', 'P4', 'P5', 'P6']
    clasica_base_headers = ['TARIFA', 'POTENCIA CONTRATADA', 'P1', 'P2', 'P3', 'P4', 'P5', 'P6']
    unica_headers = ['TARIFA', 'POTENCIA CONTRATADA', 'P1 - P6']

    # Build normalized text once
    norm = normalize_text(text)

    # Regex patterns tolerant to accents/spacing variants (applied on normalized text)
    potencia_start_rx = re.compile(r"termino\s+de\s+potencia\s*\(\s*€/?\s*\/??\s*kw\s*y\s*dia\s*\)")
    potencia_end_rx = re.compile(r"estos\s+precios\s+llevar?n?\s+incluidos")
    potencia_title_rx = re.compile(r"precio\s+potencia\s*\(\s*€/?\s*kw\s*y\s*dia\s*\)")

    # Match TE followed by any single digit (e.g., TE1, TE3)
    clasica_base_start_rx = re.compile(r"precio\s+clasica\s+base\s+te\s*\d\s*\(\s*c€/?\s*kwh\s*\)")
    clasica_base_end_rx = re.compile(r"precio\s+clasica\s+base\s+te\s*\d\s+unica")
    clasica_base_title_rx = re.compile(r"precio\s+clasica\s+base\s+te\s*\d+\s*\(\s*c€/?\s*kwh\s*\)")

    unica_start_rx = re.compile(r"precio\s+clasica\s+base\s+te\s*\d\s+unica\s*\(\s*c€/?\s*kwh\s*\)")
    unica_end_rx = re.compile(r"en\s+caso\s+de\s+no\s+marcar\s+la\s+casilla")

    potencia_data = parse_table_to_columns(text, norm, potencia_headers, potencia_start_rx, potencia_end_rx)
    clasica_base_data = parse_table_to_columns(text, norm, clasica_base_headers, clasica_base_start_rx, clasica_base_end_rx)

    # UNICA: prefer last occurrence
    last_unica_m = None
    for match in unica_start_rx.finditer(norm):
        last_unica_m = match
    unica_data_raw = {}
    if last_unica_m:
        unica_slice = norm[last_unica_m.start():]
        # Pass the slice as norm_text; orig_text still full text as parsing is normalization-based
        unica_data_raw = parse_table_to_columns(text, unica_slice, unica_headers, re.compile(r"^" + unica_start_rx.pattern), unica_end_rx)
    else:
        logger.warning("Could not find any occurrence of the UNICA table marker (regex).")

    unica_data_expanded = expand_unica_table(unica_data_raw)

    # Detect dynamic titles from the document lines (fallback to defaults if not found)
    titulo_potencia = find_title_line(text, potencia_title_rx) or 'PRECIO POTENCIA (€/kWdía)'
    titulo_base = find_title_line(text, clasica_base_title_rx) or 'PRECIO CLÁSICA BASE TE3 (c€/kWh)'
    titulo_unica = find_title_line(text, unica_start_rx) or 'PRECIO CLÁSICA BASE TE3 UNICA (c€/kWh)'

    # Extract company name for filename - hardcoded to "Total Energies"
    company_name = "Total Energies"

    # Build the required JSON schema
    result = {
        'filename': company_name,
        'termino_de_potencia': {
            'titulo': 'TÉRMINO DE POTENCIA (€/kW y día)',
            'tabla_precio_potencia': {
                'titulo': titulo_potencia,
                'tarifas': columns_to_tarifas(potencia_data)
            }
        },
        'termino_de_energia': {
            'titulo': 'TÉRMINO DE ENERGÍA (c€/kWh)',
            'tabla_precio_clasica_base': {
                'titulo': titulo_base,
                'tarifas': columns_to_tarifas(clasica_base_data)
            },
            'tabla_precio_clasica_unica': {
                'titulo': titulo_unica,
                'tarifas': columns_to_tarifas(unica_data_expanded)
            }
        }
    }

    return result
