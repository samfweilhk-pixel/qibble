FROM node:20-slim AS frontend-build
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt ./backend/
RUN pip install --no-cache-dir -r backend/requirements.txt

COPY backend/ ./backend/
COPY ingest_btc.py ./
COPY --from=frontend-build /app/frontend/dist ./static/

# Download seed parquet from GitHub Release (free, no LFS)
# then run incremental ingest to catch up to today
RUN mkdir -p data && \
    curl -L -o data/btc_1m.parquet \
    https://github.com/samfweilhk-pixel/qibble/releases/download/v1.0-data/btc_1m.parquet && \
    python ingest_btc.py

ENV DATA_PATH=/app/data/btc_1m.parquet
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "10000"]
