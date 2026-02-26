"""
FastAPI Backend for CHIRPS Precipitation Data Visualization
Provides API endpoints for the Next.js frontend
"""
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import os
import logging
import tempfile
import shutil

import numpy as np
import xarray as xr
from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import io

# Import configuration
from config import config

# Import utility modules
from utils.shapefile_processor import ShapefileProcessor
from utils.icasa_generator import IcasaWeatherGenerator
from utils.async_processor import generate_weather_package

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Zarr data path - use configuration
ZARR_PATH = config.ZARR_PATH

# Validate configuration on startup
try:
    config.validate()
    logger.info(f"Configuration validated successfully")
    logger.info(f"Zarr path: {ZARR_PATH}")
    logger.info(f"Max shapefile points: {config.MAX_SHAPEFILE_POINTS}")
    logger.info(f"Batch size: {config.BATCH_SIZE}")
except Exception as e:
    logger.error(f"Configuration validation failed: {e}")
    raise

app = FastAPI(
    title="CHIRPS Precipitation API",
    description="API for accessing CHIRPS precipitation data from Zarr store",
    version="1.0.0"
)

# Allow CORS for Next.js frontend (localhost and network access)
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
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
        # Use configurable chunks for optimal performance
        return xr.open_zarr(
            ZARR_PATH,
            chunks={
                'time': config.ZARR_TIME_CHUNKS,
                'latitude': config.ZARR_LAT_CHUNKS,
                'longitude': config.ZARR_LON_CHUNKS
            }
        )
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
        
        # Round to 1 decimal place to match display format (consistent with ICASA output)
        # This ensures 0.09 rounds to 0.1 and counts as a wet day
        precip_rounded = np.round(precip_computed, 1)
        
        # Calculate statistics on actual daily values from Zarr (all days)
        all_days_stats = {
            "total_precipitation": float(precip_rounded.sum()),
            "mean_daily": float(precip_rounded.mean()),
            "median_daily": float(np.median(precip_rounded)),
            "max_daily": float(precip_rounded.max()),
            "min_daily": float(precip_rounded.min()),
            "std_daily": float(precip_rounded.std()),
            "days_with_rain": int((precip_rounded >= 0.1).sum()),
            "dry_days": int((precip_rounded < 0.1).sum()),
        }
        
        # Calculate statistics for wet days only (excluding dry days)
        # Filter for wet days (precipitation >= 0.1 mm)
        wet_days_mask = precip_rounded >= 0.1
        wet_days_precip = precip_rounded[wet_days_mask]
        
        # Check if there are any wet days
        if len(wet_days_precip) > 0:
            wet_days_stats = {
                "total_precipitation": float(wet_days_precip.sum()),
                "mean_daily": float(wet_days_precip.mean()),
                "median_daily": float(np.median(wet_days_precip)),
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
    """Download precipitation data in ICASA format for a single location"""
    try:
        ds = open_zarr()
        
        # Use the ICASA generator for consistency
        generator = IcasaWeatherGenerator(ds)
        content = generator.generate_icasa_file(
            lat=lat,
            lon=lon,
            start_date=start_date,
            end_date=end_date,
            variables=config.DEFAULT_VARIABLES,
            site_code=config.DEFAULT_SITE_CODE
        )
        
        # Create response
        filename = IcasaWeatherGenerator.create_filename(
            lat=lat,
            lon=lon,
            start_date=start_date,
            end_date=end_date
        )
        
        return StreamingResponse(
            io.BytesIO(content.encode()),
            media_type="text/plain",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        logger.error(f"Error generating ICASA file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/download/icasa-multi")
async def download_icasa_multi(
    shapefile: UploadFile = File(..., description="Shapefile (.shp)"),
    start_date: str = Form(...),
    end_date: str = Form(...)
):
    """
    Download ICASA weather data for multiple points from a shapefile.
    Extracts all points from the shapefile and creates one ICASA file per point.
    Returns a zip file containing individual ICASA files for each coordinate.
    
    Note: Only the .shp file is required. Missing .shx and .dbf files will be auto-generated.
    """
    temp_dir = None
    
    try:
        # Read uploaded file
        shp_content = await shapefile.read()
        
        # Save shapefile
        filename = shapefile.filename or 'shapefile.shp'
        shp_path = ShapefileProcessor.save_uploaded_shapefile(
            uploaded_file=shp_content,
            filename=filename
        )
        temp_dir = shp_path.parent
        
        # Extract coordinates
        logger.info(f"Extracting coordinates from shapefile: {filename}")
        coordinates = ShapefileProcessor.extract_coordinates_from_shapefile(shp_path)
        
        # Validate coordinates
        validation = ShapefileProcessor.validate_coordinates(
            coordinates,
            max_points=config.MAX_SHAPEFILE_POINTS,
            lat_bounds=config.LAT_BOUNDS,
            lon_bounds=config.LON_BOUNDS
        )
        if not validation['valid']:
            raise HTTPException(
                status_code=400,
                detail=validation['message']
            )
        
        logger.info(f"Processing {len(coordinates)} coordinates")
        
        # Filter to valid coordinates only
        valid_coords = [
            (lon, lat) for lon, lat in coordinates
            if -90 <= lat <= 90 and -180 <= lon <= 180
        ]
        
        if len(valid_coords) == 0:
            raise HTTPException(
                status_code=400,
                detail="No valid coordinates found in shapefile"
            )
        
        # Open dataset
        ds = open_zarr()
        
        # Generate weather package
        logger.info("Generating weather data package...")
        zip_content = await generate_weather_package(
            dataset=ds,
            coordinates=valid_coords,
            start_date=start_date,
            end_date=end_date,
            variables=config.DEFAULT_VARIABLES,
            max_workers=config.MAX_WORKERS,
            batch_size=config.BATCH_SIZE
        )
        
        logger.info(f"Successfully generated package with {len(valid_coords)} files")
        
        # Create response
        filename = f"weather_data_{start_date}_{end_date}.zip"
        
        return StreamingResponse(
            io.BytesIO(zip_content),
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing shapefile: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing shapefile: {str(e)}")
    finally:
        # Clean up temporary directory
        if temp_dir and temp_dir.exists():
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Failed to clean up temporary directory: {e}")


@app.post("/api/validate-shapefile")
async def validate_shapefile(
    shapefile: UploadFile = File(..., description="Shapefile (.shp)")
):
    """
    Validate a shapefile and return coordinate information without processing.
    Useful for preview before downloading.
    
    Note: Only the .shp file is required. Missing .shx and .dbf files will be auto-generated.
    """
    temp_dir = None
    
    try:
        # Read uploaded file
        shp_content = await shapefile.read()
        
        # Save shapefile
        filename = shapefile.filename or 'shapefile.shp'
        shp_path = ShapefileProcessor.save_uploaded_shapefile(
            uploaded_file=shp_content,
            filename=filename
        )
        temp_dir = shp_path.parent
        
        # Extract coordinates
        coordinates = ShapefileProcessor.extract_coordinates_from_shapefile(shp_path)
        
        # Validate coordinates
        validation = ShapefileProcessor.validate_coordinates(
            coordinates,
            max_points=config.MAX_SHAPEFILE_POINTS,
            lat_bounds=config.LAT_BOUNDS,
            lon_bounds=config.LON_BOUNDS
        )
        
        # Add sample coordinates for preview (first 5)
        sample_coords = coordinates[:5]
        
        return {
            "valid": validation['valid'],
            "message": validation['message'],
            "total_points": len(coordinates),
            "valid_points": validation.get('valid_points', len(coordinates)),
            "invalid_points": validation.get('invalid_points', 0),
            "sample_coordinates": [
                {"lon": lon, "lat": lat} for lon, lat in sample_coords
            ],
            "issues": validation.get('issues', [])
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error validating shapefile: {e}")
        raise HTTPException(status_code=500, detail=f"Error validating shapefile: {str(e)}")
    finally:
        # Clean up temporary directory
        if temp_dir and temp_dir.exists():
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Failed to clean up temporary directory: {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
