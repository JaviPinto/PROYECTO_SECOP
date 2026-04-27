FROM ghcr.io/dask/dask:latest

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline.py .
COPY scripts/ ./scripts/