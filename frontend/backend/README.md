# CHIRPS + NASA POWER Weather API

FastAPI backend for serving CHIRPS precipitation and NASA POWER meteorological data from Zarr storage.

## Overview

This backend provides RESTful API endpoints for accessing and analyzing weather data. It handles:

- Spatial and temporal subsetting
- Data aggregation (daily, weekly, monthly, yearly)
- Statistical calculations
- Single-point and multi-point ICASA exports
- Spatial upload validation (shapefile, GeoJSON, zip)
- NASA POWER integration and cache warming

## Installation

```bash
pip install -r requirements.txt
```

## Running the Server

```bash
uvicorn main:app --reload --port 8000
```

Or with custom host/port:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

## API Documentation

Once running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Endpoints

### GET /
Health check endpoint

### GET /api/metadata
Get metadata about the Zarr store including:
- Time range (start, end, total days)
- Spatial extent (lat/lon bounds and resolution)
- Available variables
- Dimensions

### GET /api/variables
Get all weather variables available for plotting and export.

### POST /api/data/preload-weather-cache
Warm NASA POWER cache for a date range.

### POST /api/data/timeseries
Get precipitation time series for a location/region

**Request Body:**
```json
{
  "bounds": {
    "lon_min": -10.0,
    "lon_max": -9.0,
    "lat_min": 5.0,
    "lat_max": 6.0
  },
  "date_range": {
    "start_date": "2024-01-01",
    "end_date": "2024-12-31"
  },
  "aggregation": "monthly"
}
```

**Response:**
```json
{
  "time": ["2024-01-01", "2024-02-01", ...],
  "precipitation": [45.2, 68.3, ...],
  "units": "mm",
  "aggregation": "monthly"
}
```

### POST /api/data/timeseries-variable
Get time series for a specific variable (RAIN, TMAX, TMIN, T2M, SRAD, WIND, TDEW, RH2M, etc.).

### POST /api/data/statistics
Get statistical summary for a location and date range

**Request Body:**
```json
{
  "bounds": {
    "lon_min": -10.0,
    "lon_max": -9.0,
    "lat_min": 5.0,
    "lat_max": 6.0
  },
  "date_range": {
    "start_date": "2024-01-01",
    "end_date": "2024-12-31"
  }
}
```

**Response:**
```json
{
  "all_days": {
    "total_precipitation": 1234.5,
    "mean_daily": 3.38,
    "median_daily": 2.1,
    "max_daily": 45.6,
    "min_daily": 0.0,
    "std_daily": 4.2,
    "days_with_rain": 180,
    "dry_days": 185
  },
  "wet_days": {
    "total_precipitation": 1234.5,
    "mean_daily": 6.86,
    "median_daily": 5.8,
    "max_daily": 45.6,
    "min_daily": 0.1,
    "std_daily": 4.8,
    "days_with_rain": 180,
    "dry_days": 0
  }
}
```

### POST /api/data/spatial
Get spatial precipitation data around a point

**Query Parameters:**
- `lat`: Latitude
- `lon`: Longitude
- `start_date`: Start date (YYYY-MM-DD)
- `end_date`: End date (YYYY-MM-DD)
- `resolution`: Spatial extent in degrees (optional)

### POST /api/download/icasa
Download data in ICASA weather format

**Query Parameters:**
- `lat`: Latitude
- `lon`: Longitude
- `start_date`: Start date (YYYY-MM-DD)
- `end_date`: End date (YYYY-MM-DD)

**Returns:** Single `.WTH` file download (filename uses deterministic 8-character point hash).

### POST /api/download/icasa-multi
Download ICASA weather files for multiple points from uploaded shapefile/GeoJSON/zip.

Behavior:
- Uses uploaded point IDs when available (`id`, `point_id`, `pid`, `cell_id`)
- Generates deterministic 8-character hash IDs only when IDs are missing
- Includes generated ID manifest as GeoJSON under `shapefile/` when fallback IDs are used

### POST /api/validate-shapefile
Validate uploaded spatial files and preview coordinate counts before processing.

## CORS Configuration

CORS origins are configured in `config.py` (`Config.CORS_ORIGINS`).

## Error Handling

All endpoints return appropriate HTTP status codes:
- 200: Success
- 400: Bad request (invalid parameters)
- 404: Resource not found
- 500: Internal server error

Error responses include a `detail` field with the error message.

## Performance Considerations

- Data is loaded lazily using xarray
- Spatial and temporal subsetting is performed on chunked arrays
- Aggregations use Dask for parallel computation
- Consider adding caching for frequently requested data

## Environment Variables

Key environment variables (see `config.py` for full list):

- `ENABLE_NASA_POWER` (default: `true`)
- `DEFAULT_RAIN_SOURCE` (`chirps`, `nasa_power`, `both`)
- `MAX_SHAPEFILE_POINTS` (default: `1000`)
- `BATCH_SIZE` (default: `50`)
- `MAX_WORKERS` (optional)
- `LOG_LEVEL` (default: `INFO`)
- `NASA_POWER_VERIFY_SSL` (default: `true`)
- `NASA_POWER_SSL_CERT_PATH` (optional)
- `DEFAULT_SITE_CODE` (default: `UFLC`)

## Dependencies

Install from `requirements.txt` in this directory.

## Development

To enable debug mode:
```python
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True, log_level="debug")
```

## Testing

Test endpoints using curl:

```bash
# Health check
curl http://localhost:8000/

# Get metadata
curl http://localhost:8000/api/metadata

# Get time series
curl -X POST http://localhost:8000/api/data/timeseries \
  -H "Content-Type: application/json" \
  -d '{
    "bounds": {"lon_min": -10, "lon_max": -9, "lat_min": 5, "lat_max": 6},
    "date_range": {"start_date": "2024-01-01", "end_date": "2024-01-31"},
    "aggregation": "daily"
  }'
```

## Production Deployment

For production deployment:

1. Use a production ASGI server (Gunicorn with Uvicorn workers):
```bash
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker
```

2. Set up reverse proxy (nginx)
3. Enable HTTPS
4. Configure logging
5. Set up monitoring
6. Consider adding rate limiting
7. Add authentication for sensitive operations
