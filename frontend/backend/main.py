"""
FastAPI Backend for CHIRPS Precipitation Data Visualization
Provides API endpoints for the Next.js frontend
"""
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import os

import numpy as np
import xarray as xr
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import io

# Zarr data path - use absolute path resolution
BACKEND_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BACKEND_DIR.parent.parent
ZARR_PATH = PROJECT_ROOT / "data" / "zarr" / "chirps_v3.0_daily_precip_v1.0.zarr"

print(f"Backend directory: {BACKEND_DIR}")
print(f"Project root: {PROJECT_ROOT}")
print(f"Zarr path: {ZARR_PATH}")
print(f"Zarr exists: {ZARR_PATH.exists()}")

app = FastAPI(
    title="CHIRPS Precipitation API",
    description="API for accessing CHIRPS precipitation data from Zarr store",
    version="1.0.0"
)

# Allow CORS for Next.js frontend (localhost and network access)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", 
        "http://localhost:3001",
        "http://10.138.107.50:3000",
        "http://10.138.107.50:3001"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helper function to open dataset
def open_zarr():
    """Open the Zarr dataset"""
    if not ZARR_PATH.exists():
        raise HTTPException(status_code=500, detail=f"Zarr store not found: {ZARR_PATH}")
    try:
        # Zarr 3.1.5 natively supports v3 format
        # Use smaller chunks for faster single-point access
        return xr.open_zarr(ZARR_PATH, chunks={'time': 100, 'latitude': 100, 'longitude': 100})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error opening Zarr store: {str(e)}")


class SpatialBounds(BaseModel):
    lon_min: float
    lon_max: float
    lat_min: float
    lat_max: float


class DateRange(BaseModel):
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD


class DataRequest(BaseModel):
    bounds: SpatialBounds
    date_range: DateRange
    aggregation: Optional[str] = None  # 'daily', 'weekly', 'monthly', 'yearly'


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "operational",
        "service": "CHIRPS Precipitation API",
        "version": "1.0.0"
    }


@app.get("/api/metadata")
async def get_metadata():
    """Get metadata about the Zarr store"""
    try:
        ds = open_zarr()
        
        # Get coordinate ranges
        time_values = ds.time.values
        lat_values = ds.latitude.values
        lon_values = ds.longitude.values
        
        return {
            "time_range": {
                "start": str(time_values[0]),
                "end": str(time_values[-1]),
                "total_days": len(time_values)
            },
            "spatial_extent": {
                "latitude": {
                    "min": float(lat_values.min()),
                    "max": float(lat_values.max()),
                    "resolution": float(abs(lat_values[1] - lat_values[0]))
                },
                "longitude": {
                    "min": float(lon_values.min()),
                    "max": float(lon_values.max()),
                    "resolution": float(abs(lon_values[1] - lon_values[0]))
                }
            },
            "variables": list(ds.data_vars),
            "dimensions": dict(ds.dims)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data/timeseries")
async def get_timeseries(request: DataRequest):
    """Get precipitation time series for a location/region"""
    try:
        ds = open_zarr()
        
        # Use nearest neighbor to get exact grid cell value (no averaging)
        center_lon = (request.bounds.lon_min + request.bounds.lon_max) / 2
        center_lat = (request.bounds.lat_min + request.bounds.lat_max) / 2
        
        # IMPORTANT: Select spatial point FIRST (reduces data), then time range
        # This is much faster than selecting time first
        data = ds.sel(
            longitude=center_lon,
            latitude=center_lat,
            method='nearest'
        ).sel(
            time=slice(request.date_range.start_date, request.date_range.end_date)
        )
        
        # Get actual precipitation values from Zarr (no alterations)
        precip = data.precipitation
        
        # Ensure we have data before attempting aggregation
        if len(precip.time) == 0:
            raise HTTPException(
                status_code=400, 
                detail="No data found for the specified location and date range"
            )
        
        # Apply temporal aggregation if requested
        if request.aggregation == 'weekly':
            # Only resample if we have enough data
            if len(precip.time) > 0:
                precip = precip.resample(time='W').mean()
        elif request.aggregation == 'monthly':
            # Only resample if we have enough data
            if len(precip.time) > 0:
                precip = precip.resample(time='ME').mean()
        elif request.aggregation == 'yearly':
            # Only resample if we have enough data
            if len(precip.time) > 0:
                precip = precip.resample(time='YE').mean()
        
        # Compute values
        precip_computed = precip.compute()
        
        # Convert to JSON-serializable format (preserve exact values)
        time_values = [str(t) for t in precip_computed.time.values]
        precip_values = [
            float(v) if not np.isnan(v) else None 
            for v in precip_computed.values
        ]
        
        return {
            "time": time_values,
            "precipitation": precip_values,
            "units": "mm/day",
            "aggregation": request.aggregation or "daily"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data/statistics")
async def get_statistics(request: DataRequest):
    """Get statistical summary for a location/region and date range"""
    try:
        ds = open_zarr()
        
        # Use nearest neighbor to get exact grid cell value (no averaging)
        center_lon = (request.bounds.lon_min + request.bounds.lon_max) / 2
        center_lat = (request.bounds.lat_min + request.bounds.lat_max) / 2
        
        # Select spatial point FIRST (faster), then time range
        data = ds.sel(
            longitude=center_lon,
            latitude=center_lat,
            method='nearest'
        ).sel(
            time=slice(request.date_range.start_date, request.date_range.end_date)
        )
        
        # Get actual precipitation values from Zarr (no alterations)
        precip = data.precipitation
        
        # Compute the precipitation data once to avoid dask boolean indexing issues
        precip_computed = precip.compute()
        
        # Calculate statistics on actual daily values from Zarr (all days)
        all_days_stats = {
            "total_precipitation": float(precip_computed.sum()),
            "mean_daily": float(precip_computed.mean()),
            "median_daily": float(precip_computed.quantile(0.5)),
            "max_daily": float(precip_computed.max()),
            "min_daily": float(precip_computed.min()),
            "std_daily": float(precip_computed.std()),
            "days_with_rain": int((precip_computed > 0.1).sum()),
            "dry_days": int((precip_computed <= 0.1).sum()),
        }
        
        # Calculate statistics for wet days only (excluding dry days)
        # Filter for wet days (precipitation > 0.1 mm)
        wet_days_mask = precip_computed > 0.1
        wet_days_precip = precip_computed[wet_days_mask]
        
        # Check if there are any wet days
        if len(wet_days_precip) > 0:
            wet_days_stats = {
                "total_precipitation": float(wet_days_precip.sum()),
                "mean_daily": float(wet_days_precip.mean()),
                "median_daily": float(wet_days_precip.quantile(0.5)),
                "max_daily": float(wet_days_precip.max()),
                "min_daily": float(wet_days_precip.min()),
                "std_daily": float(wet_days_precip.std()),
                "days_with_rain": int(len(wet_days_precip)),
                "dry_days": 0,
            }
        else:
            # No wet days in the dataset
            wet_days_stats = {
                "total_precipitation": 0.0,
                "mean_daily": 0.0,
                "median_daily": 0.0,
                "max_daily": 0.0,
                "min_daily": 0.0,
                "std_daily": 0.0,
                "days_with_rain": 0,
                "dry_days": 0,
            }
        
        return {
            "all_days": all_days_stats,
            "wet_days": wet_days_stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data/spatial")
async def get_spatial_data(
    lat: float = Query(...),
    lon: float = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    resolution: Optional[float] = Query(None)
):
    """Get spatial precipitation data around a point"""
    try:
        # Define spatial extent based on resolution/zoom level
        extent = resolution or 0.5  # Default 0.5 degrees
        
        ds = open_zarr()
        
        data = ds.sel(
            longitude=slice(lon - extent, lon + extent),
            latitude=slice(lat + extent, lat - extent),
            time=slice(start_date, end_date)
        )
        
        # Calculate temporal mean
        mean_precip = data.precipitation.mean(dim='time').compute()
        
        return {
            "latitude": data.latitude.values.tolist(),
            "longitude": data.longitude.values.tolist(),
            "precipitation": mean_precip.values.tolist(),
            "units": "mm/day"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/download/icasa")
async def download_icasa(
    lat: float = Query(...),
    lon: float = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    """Download precipitation data in ICASA format"""
    try:
        ds = open_zarr()
        
        # Select spatial point FIRST (faster), then time range
        data = ds.sel(
            longitude=lon,
            latitude=lat,
            method='nearest'
        ).sel(
            time=slice(start_date, end_date)
        )
        
        # Get actual precipitation values from Zarr (no alterations)
        precip = data.precipitation.compute()
        
        # Create ICASA formatted output
        output = io.StringIO()
        output.write("$WEATHER DATA: UF\n\n")
        output.write("! RAIN     Precipitation Corrected (mm/day)\n\n")
        output.write(f"@ INSI   WTHLAT  WTHLONG\n")
        output.write(f"  UFLC     {lat:.1f}    {lon:.1f}\n\n")
        output.write("@  DATE   RAIN\n")
        
        # Write daily data in ICASA format (DDDYYYY) - exact values from Zarr
        for time_val, precip_val in zip(precip.time.values, precip.values):
            dt = datetime.fromisoformat(str(time_val)[:10])
            # Format: YYYYDDDD (year +day of year)
            date_str = dt.strftime("%Y%j")
            # Use exact value from Zarr, only format for display (1 decimal)
            precip_formatted = f"{float(precip_val):.1f}" if not np.isnan(precip_val) else "0.0"
            output.write(f"{date_str}    {precip_formatted}\n")
        
        # Create response
        output.seek(0)
        filename = f"weather_{lat}_{lon}_{start_date}_{end_date}.txt"
        
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type="text/plain",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
