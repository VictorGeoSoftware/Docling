FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY docling_price_tables_extraction_api_server.py /app/docling_price_tables_extraction_api_server.py
COPY price_table_extraction.py /app/price_table_extraction.py

ENV PORT=5010
EXPOSE 5010

CMD ["python", "/app/docling_price_tables_extraction_api_server.py"]
