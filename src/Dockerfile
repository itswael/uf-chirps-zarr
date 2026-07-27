# Ingestion (CLI) Dockerfile
FROM python:3.11-slim

# System deps for rasterio/geopandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    git \
    curl \
    ca-certificates \
    libgeos-dev \
    libproj-dev \
    proj-bin \
    libgdal-dev \
    gdal-bin \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install shared requirements
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy ingestion source
COPY src /app/src
COPY config /app/config
COPY .env.example /app/.env.example

# Create default data dirs (bind mount will override)
RUN mkdir -p /data/raw /data/interim /data/zarr

# Set defaults; override in docker-compose
ENV CHIRPS_BASE_DIR=/app \
    CHIRPS_RAW_DIR=data/raw \
    CHIRPS_INTERIM_DIR=data/interim \
    CHIRPS_ZARR_DIR=data/zarr \
    PYTHONUNBUFFERED=1

# Provide an entrypoint that passes through to CLI
ENTRYPOINT ["python", "-m", "src.cli"]

# Default to show info; compose will override with arguments (e.g., "auto -y")
CMD ["info"]
