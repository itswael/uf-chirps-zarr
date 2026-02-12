"""
FastAPI Backend for CHIRPS Precipitation Data Visualization
Provides API endpoints for the Next.js frontend
"""
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import numpy as np
import xarray as xr
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import io

# Zarr data path
ZARR_PATH = Path(__file__).parent.parent.parent / "data" / "zarr" / "chirps_v3.0_daily_precip_v1.0.zarr"

app = FastAPI(
    title="CHIRPS Precipitation API",
    description="API for accessing CHIRPS precipitation data from Zarr store",
    version="1.0.0"
)

# Allow CORS for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helper function to open dataset
def open_zarr():
    """Open the Zarr dataset"""
    if not ZARR_PATH.exists():
        raise HTTPException(status_code=500, detail=f"Zarr store not found: {ZARR_PATH}")
    return xr.open_zarr(ZARR_PATH, chunks={'time': 30, 'latitude': 500, 'longitude': 500})


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
        
        # Get spatial subset
        data = ds.sel(
            longitude=slice(request.bounds.lon_min, request.bounds.lon_max),
            latitude=slice(request.bounds.lat_max, request.bounds.lat_min),  # Latitude is descending
            time=slice(request.date_range.start_date, request.date_range.end_date)
        )
        
        # Calculate spatial mean
        precip = data.precipitation.mean(dim=['latitude', 'longitude'])
        
        # Apply temporal aggregation if requested
        if request.aggregation == 'weekly':
            precip = precip.resample(time='W').sum()
        elif request.aggregation == 'monthly':
            precip = precip.resample(time='M').sum()
        elif request.aggregation == 'yearly':
            precip = precip.resample(time='Y').sum()
        
        # Compute values
        precip_computed = precip.compute()
        
        # Convert to JSON-serializable format
        time_values = [str(t) for t in precip_computed.time.values]
        precip_values = [
            float(v) if not np.isnan(v) else None 
            for v in precip_computed.values
        ]
        
        return {
            "time": time_values,
            "precipitation": precip_values,
            "units": "mm/day" if request.aggregation == 'daily' else "mm",
            "aggregation": request.aggregation or "daily"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data/statistics")
async def get_statistics(request: DataRequest):
    """Get statistical summary for a location/region and date range"""
    try:
        ds = open_zarr()
        
        data = ds.sel(
            longitude=slice(request.bounds.lon_min, request.bounds.lon_max),
            latitude=slice(request.bounds.lat_max, request.bounds.lat_min),
            time=slice(request.date_range.start_date, request.date_range.end_date)
        )
        
        precip = data.precipitation
        
        # Calculate statistics
        stats = {
            "total_precipitation": float(precip.sum().compute()),
            "mean_daily": float(precip.mean().compute()),
            "median_daily": float(precip.median().compute()),
            "max_daily": float(precip.max().compute()),
            "min_daily": float(precip.min().compute()),
            "std_daily": float(precip.std().compute()),
            "days_with_rain": int((precip > 0.1).sum().compute()),
            "dry_days": int((precip <= 0.1).sum().compute()),
        }
        
        return stats
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
        
        # Get data for nearest grid point
        data = ds.sel(
            longitude=slice(lon - 0.05, lon + 0.05),
            latitude=slice(lat + 0.05, lat - 0.05),
            time=slice(start_date, end_date)
        )
        
        # Extract precipitation values
        precip = data.precipitation.mean(dim=['latitude', 'longitude']).compute()
        
        # Create ICASA formatted output
        output = io.StringIO()
        output.write("$WEATHER DATA: UF\n\n")
        output.write("! RAIN     Precipitation Corrected (mm/day)\n\n")
        output.write(f"@ INSI   WTHLAT  WTHLONG\n")
        output.write(f"  UFLC     {lat:.1f}    {lon:.1f}\n\n")
        output.write("@  DATE   RAIN\n")
        
        # Write daily data in ICASA format (YYYYDDD)
        for time_val, precip_val in zip(precip.time.values, precip.values):
            dt = datetime.fromisoformat(str(time_val)[:10])
            # Format: YYYYDDD (year + day of year)
            date_str = dt.strftime("%Y%j")
            precip_formatted = f"{precip_val:.1f}" if not np.isnan(precip_val) else "0.0"
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
