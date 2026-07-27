# Backend Dockerfile
FROM python:3.11-slim

# Install system dependencies for geopandas, rasterio, etc.
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

# Set workdir
WORKDIR /app/backend

# Copy only backend first for better caching
COPY backend/requirements.txt /app/backend/requirements.txt

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY backend /app/backend
COPY requirements.txt /app/requirements.txt

# Expose backend port
EXPOSE 8000

# Environment for performance tuning (override at runtime as needed)
ENV MAX_WORKERS="" \
    USE_PROCESSES=false \
    LOG_LEVEL=INFO

# Default command uses gunicorn with uvicorn workers; workers configurable via env
# GUNICORN_WORKERS is used to replicate multiple instances via processes inside the container when not scaled externally
ENV GUNICORN_WORKERS=2

CMD ["/bin/sh", "-c", "gunicorn main:app -w ${GUNICORN_WORKERS} -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000 --timeout 3600 --access-logfile - --error-logfile -"]
