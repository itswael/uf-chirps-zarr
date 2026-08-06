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
Health check endpoint.

### GET /api/metadata
Returns metadata about the Zarr store, including:
- Time range and total days
- Spatial extent and resolution
- Available variables
- Dataset dimensions
- NASA POWER metadata when enabled

### GET /api/variables
Returns the variables available for plotting and export.

### POST /api/data/preload-weather-cache
Warms the NASA POWER cache for a date range.

**Query parameters:**
- `start_date`: Start date in `YYYY-MM-DD` format
- `end_date`: End date in `YYYY-MM-DD` format

### POST /api/data/timeseries
Returns CHIRPS precipitation time series for a location or region.

**Request body:**
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

### POST /api/data/timeseries-variable
Returns a time series for a specific variable such as `RAIN`, `TMAX`, `TMIN`, `T2M`, `SRAD`, `WIND`, `TDEW`, or `RH2M`.

**Query parameters:**
- `lat`: Latitude
- `lon`: Longitude
- `start_date`: Start date in `YYYY-MM-DD` format
- `end_date`: End date in `YYYY-MM-DD` format
- `variable`: Variable code
- `aggregation`: Optional `daily`, `weekly`, `monthly`, or `yearly`

### POST /api/data/statistics
Returns statistical summaries for a location and date range.

**Request body:**
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

### POST /api/data/spatial
Returns spatial precipitation data around a point.

**Query parameters:**
- `lat`: Latitude
- `lon`: Longitude
- `start_date`: Start date in `YYYY-MM-DD` format
- `end_date`: End date in `YYYY-MM-DD` format
- `resolution`: Optional spatial extent in degrees

### POST /api/download/icasa
Downloads a single-point ICASA weather file.

**Query parameters:**
- `lat`: Latitude
- `lon`: Longitude
- `start_date`: Start date in `YYYY-MM-DD` format
- `end_date`: End date in `YYYY-MM-DD` format
- `rain_source`: `chirps`, `nasa_power`, or `both`
- `selected_parameters`: Optional comma-separated ICASA variables

**Output:**
- A single `.WTH` file download with a deterministic 8-character point ID in the filename

### POST /api/download/icasa-multi
Downloads ICASA weather files for multiple points from an uploaded spatial file.

**Form fields:**
- `shapefile`: Required spatial file upload (`.shp`, `.geojson`, `.json`, or `.zip`)
- `start_date`: Start date in `YYYY-MM-DD` format
- `end_date`: End date in `YYYY-MM-DD` format
- `rain_source`: Optional, defaults to `both`
- `selected_parameters`: Optional comma-separated ICASA variables
- `shapefile_shx`: Optional `.shx` companion file
- `shapefile_dbf`: Optional `.dbf` companion file

**Behavior:**
- Uses uploaded point IDs when available (`id`, `point_id`, `pid`, `cell_id`)
- Generates deterministic 8-character hash IDs when IDs are missing
- Includes a GeoJSON manifest under `shapefile/` when fallback IDs are generated

### POST /api/validate-shapefile
Validates an uploaded spatial file and returns preview information without generating ICASA output.

**Form fields:**
- `shapefile`: Required spatial file upload (`.shp`, `.geojson`, `.json`, or `.zip`)
- `shapefile_shx`: Optional `.shx` companion file
- `shapefile_dbf`: Optional `.dbf` companion file

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
