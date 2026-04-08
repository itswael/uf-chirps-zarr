"""
FastAPI Backend for CHIRPS Precipitation Data Visualization
Provides API endpoints for the Next.js frontend
Now includes NASA POWER meteorological data integration
"""
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import os
import logging
import tempfile
import shutil
import json

import numpy as np
import pandas as pd
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

# Import NASA POWER utilities
from utils.nasa_power_fetcher import get_fetcher
from utils.weather_data_merger import WeatherDataMerger
from utils.enhanced_icasa_generator import EnhancedIcasaGenerator, EnhancedIcasaBatchGenerator
from utils.nasa_power_config import nasa_power_config
from utils.point_id import generate_point_id

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
    expose_headers=["Content-Disposition"],
)


# Startup event to initialize NASA POWER data
@app.on_event("startup")
async def startup_event():
    """Initialize NASA POWER datasets on startup"""
    if config.ENABLE_NASA_POWER:
        logger.info("Initializing NASA POWER data fetcher...")
        try:
            fetcher = get_fetcher()
            await fetcher.load_datasets()
            logger.info("NASA POWER data fetcher initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize NASA POWER fetcher: {e}")
            logger.warning("NASA POWER features will be disabled")


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


def parse_selected_parameters(selected_parameters: Optional[str]) -> Optional[List[str]]:
    """Parse and validate comma-separated selected ICASA parameter list."""
    if not selected_parameters:
        return None

    parsed = [p.strip().upper() for p in selected_parameters.split(',') if p.strip()]
    if not parsed:
        return None

    available = set(nasa_power_config.get_available_variables())
    invalid = [p for p in parsed if p not in available]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid selected_parameters: {', '.join(invalid)}. "
                f"Available: {', '.join(sorted(available))}"
            )
        )

    # De-duplicate while preserving order.
    ordered_unique = list(dict.fromkeys(parsed))
    return ordered_unique


def validate_selected_parameters_for_rain_source(
    selected_variables: Optional[List[str]],
    rain_source: str
) -> None:
    """Validate parameter choices against the selected rain source behavior."""
    if not selected_variables:
        return

    if rain_source in ['chirps', 'nasa_power'] and 'RAIN1' in selected_variables:
        raise HTTPException(
            status_code=400,
            detail=(
                "RAIN1 is only available when rain_source is 'both'. "
                "Use RAIN for chirps or nasa_power single-source selections."
            )
        )


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


@app.post("/api/data/preload-weather-cache")
async def preload_weather_cache(
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    """Warm the NASA POWER date-range cache for upcoming requests."""
    try:
        if not config.ENABLE_NASA_POWER:
            return {
                "status": "disabled",
                "message": "NASA POWER is disabled"
            }

        fetcher = get_fetcher()
        await fetcher.prepare_date_range_cache(
            start_date=pd.to_datetime(start_date).date(),
            end_date=pd.to_datetime(end_date).date(),
        )

        return {
            "status": "ready",
            "start_date": start_date,
            "end_date": end_date,
        }
    except Exception as e:
        logger.error(f"Error preloading NASA POWER cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
        
        metadata = {
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
        
        # Add NASA POWER metadata if enabled
        if config.ENABLE_NASA_POWER:
            try:
                fetcher = get_fetcher()
                nasa_meta = await fetcher.get_metadata()
                metadata['nasa_power'] = nasa_meta
                metadata['nasa_power_enabled'] = True
                metadata['available_variables'] = config.AVAILABLE_PLOT_VARIABLES
                metadata['default_plot_variable'] = config.DEFAULT_PLOT_VARIABLE
                metadata['rain_sources'] = ['chirps', 'nasa_power', 'both']
                metadata['default_rain_source'] = config.DEFAULT_RAIN_SOURCE
            except Exception as e:
                logger.warning(f"Could not fetch NASA POWER metadata: {e}")
                metadata['nasa_power_enabled'] = False
        else:
            metadata['nasa_power_enabled'] = False
        
        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/variables")
async def get_available_variables():
    """Get list of available weather variables with descriptions"""
    try:
        variables = {}
        
        # Add CHIRPS variable
        variables['RAIN'] = {
            'code': 'RAIN',
            'description': 'Precipitation from CHIRPS',
            'units': 'mm/day',
            'source': 'CHIRPS',
            'available_for_plot': True
        }
        
        # Add NASA POWER variables if enabled
        if config.ENABLE_NASA_POWER:
            for var_code in config.AVAILABLE_PLOT_VARIABLES:
                if var_code != 'RAIN':
                    var_config = nasa_power_config.get_variable_config(var_code)
                    if var_config:
                        variables[var_code] = {
                            'code': var_code,
                            'description': var_config['description'],
                            'units': var_config['units'],
                            'source': var_config['source'],
                            'available_for_plot': True
                        }
        
        return {
            'variables': variables,
            'default_plot_variable': config.DEFAULT_PLOT_VARIABLE
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


@app.post("/api/data/timeseries-variable")
async def get_timeseries_variable(
    lat: float = Query(...),
    lon: float = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    variable: str = Query(..., description="Variable code (RAIN, TMAX, TMIN, etc.)"),
    aggregation: Optional[str] = Query(None, description="daily, weekly, monthly, yearly")
):
    """Get time series for any weather variable (CHIRPS or NASA POWER)"""
    try:
        # Validate variable
        if variable not in config.AVAILABLE_PLOT_VARIABLES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid variable: {variable}. Available: {config.AVAILABLE_PLOT_VARIABLES}"
            )
        
        ds = open_zarr()
        merger = WeatherDataMerger(ds)
        
        # Get merged data
        df = await merger.merge_weather_data(
            lat=lat,
            lon=lon,
            start_date=start_date,
            end_date=end_date,
            rain_source="both",  # Get all data
            include_solar=True,
            include_met=True
        )
        
        # Check if variable exists in data
        if variable not in df.columns:
            raise HTTPException(
                status_code=404,
                detail=f"Variable {variable} not available in data"
            )
        
        # Apply aggregation if requested
        if aggregation and aggregation != 'daily':
            df['time'] = pd.to_datetime(df['time'])
            df = df.set_index('time')
            
            if aggregation == 'weekly':
                df = df.resample('W').mean()
            elif aggregation == 'monthly':
                df = df.resample('ME').mean()
            elif aggregation == 'yearly':
                df = df.resample('YE').mean()
            
            df = df.reset_index()
        
        # Get variable metadata
        var_config = nasa_power_config.get_variable_config(variable)
        if variable == 'RAIN':
            units = 'mm/day'
            description = 'CHIRPS Precipitation'
        elif var_config:
            units = var_config['units']
            description = var_config['description']
        else:
            units = 'unknown'
            description = variable
        
        # Convert to JSON format
        time_values = [str(t) for t in df['time']]
        var_values = [
            float(v) if not pd.isna(v) else None
            for v in df[variable]
        ]
        
        return {
            "time": time_values,
            "values": var_values,
            "variable": variable,
            "units": units,
            "description": description,
            "aggregation": aggregation or "daily"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching variable time series: {e}")
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
    end_date: str = Query(...),
    rain_source: str = Query("both", description="Rain data source: chirps, nasa_power, or both"),
    selected_parameters: Optional[str] = Query(
        None,
        description="Comma-separated ICASA parameters to include (e.g., RAIN,TMAX,TMIN)"
    )
):
    """
    Download weather data in ICASA format for a single location.
    Includes CHIRPS precipitation and NASA POWER meteorological data.
    """
    try:
        # Validate rain source
        if rain_source not in ['chirps', 'nasa_power', 'both']:
            raise HTTPException(
                status_code=400,
                detail="rain_source must be one of: chirps, nasa_power, both"
            )
        
        ds = open_zarr()
        merger = WeatherDataMerger(ds)
        selected_vars = parse_selected_parameters(selected_parameters)
        validate_selected_parameters_for_rain_source(selected_vars, rain_source)
        
        # Get merged data
        df = await merger.merge_weather_data(
            lat=lat,
            lon=lon,
            start_date=start_date,
            end_date=end_date,
            rain_source=rain_source,
            include_solar=True,
            include_met=True
        )
        
        # Generate ICASA file using enhanced generator
        generator = EnhancedIcasaGenerator()
        
        source_desc = "CHIRPS + NASA POWER"
        if rain_source == "chirps":
            source_desc = "CHIRPS + NASA POWER (Rain: CHIRPS)"
        elif rain_source == "nasa_power":
            source_desc = "NASA POWER"
        
        content = generator.generate_icasa_content(
            df=df,
            lat=lat,
            lon=lon,
            site_code=config.DEFAULT_SITE_CODE,
            source_description=source_desc,
            selected_variables=selected_vars,
        )
        
        # Single-point files always use deterministic hash IDs.
        point_id = generate_point_id(lat=lat, lon=lon, length=8)
        filename = generator.create_filename(
            lat=lat,
            lon=lon,
            start_date=start_date,
            end_date=end_date,
            point_id=point_id,
        )
        
        return StreamingResponse(
            io.BytesIO(content.encode()),
            media_type="text/plain",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating ICASA file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/download/icasa-multi")
async def download_icasa_multi(
    shapefile: UploadFile = File(..., description="Shapefile (.shp, .geojson, .json, or .zip)"),
    start_date: str = Form(...),
    end_date: str = Form(...),
    rain_source: str = Form("both", description="Rain data source: chirps, nasa_power, or both"),
    selected_parameters: Optional[str] = Form(
        None,
        description="Comma-separated ICASA parameters to include (e.g., RAIN,TMAX,TMIN)"
    ),
    shapefile_shx: Optional[UploadFile] = File(None, description="Shapefile .shx companion file"),
    shapefile_dbf: Optional[UploadFile] = File(None, description="Shapefile .dbf companion file")
):
    """
    Download ICASA weather data for multiple points from a spatial file.
    Supports shapefiles (.shp with .shx and .dbf), GeoJSON, and zip archives.
    Returns a zip file containing individual ICASA files for each coordinate.
    
    Shapefile users can upload:
    - A .zip file containing .shp, .shx, and .dbf files
    - Just the .shp file with optional .shx and .dbf files as companion uploads
    
    GeoJSON users can upload:
    - A .geojson or .json file directly
    """
    temp_dir = None
    
    try:
        # Validate rain source
        if rain_source not in ['chirps', 'nasa_power', 'both']:
            raise HTTPException(
                status_code=400,
                detail="rain_source must be one of: chirps, nasa_power, both"
            )

        selected_vars = parse_selected_parameters(selected_parameters)
        validate_selected_parameters_for_rain_source(selected_vars, rain_source)
        
        # Read uploaded file
        shp_content = await shapefile.read()
        
        # Collect additional files if provided
        additional_files = {}
        if shapefile_shx:
            shx_content = await shapefile_shx.read()
            additional_files[shapefile_shx.filename or 'file.shx'] = shx_content
        
        if shapefile_dbf:
            dbf_content = await shapefile_dbf.read()
            additional_files[shapefile_dbf.filename or 'file.dbf'] = dbf_content
        
        # Save spatial file
        filename = shapefile.filename or 'spatial_file'
        spatial_path = ShapefileProcessor.save_uploaded_shapefile(
            uploaded_file=shp_content,
            filename=filename,
            additional_files=additional_files if additional_files else None
        )
        temp_dir = spatial_path.parent
        
        # Extract coordinates and point IDs from file
        logger.info(f"Extracting coordinates and point IDs from: {filename}")
        coordinates, point_ids_mapping, extraction_metadata = ShapefileProcessor.extract_coordinates_and_ids_from_file(spatial_path)
        
        # Validate coordinates with IDs
        validation = ShapefileProcessor.validate_coordinates_with_ids(
            coordinates,
            point_ids_mapping,
            max_points=config.MAX_SHAPEFILE_POINTS,
            lat_bounds=config.LAT_BOUNDS,
            lon_bounds=config.LON_BOUNDS
        )
        if not validation['valid']:
            raise HTTPException(
                status_code=400,
                detail=validation['message']
            )
        
        logger.info(f"Processing {len(coordinates)} coordinates with {len(set(point_ids_mapping.values()))} unique point IDs")
        
        # Filter to valid coordinates only (keeping track of valid indices)
        generated_id_indices = set(extraction_metadata.get('generated_id_indices', []))
        valid_indices = []
        valid_coords = []
        has_generated_ids = False
        point_id_features = []
        for idx, (lon, lat) in enumerate(coordinates):
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                valid_indices.append(idx)
                valid_coords.append((lon, lat))

                point_id = str(point_ids_mapping[idx])
                generated_for_point = idx in generated_id_indices
                has_generated_ids = has_generated_ids or generated_for_point
                point_id_features.append({
                    "type": "Feature",
                    "properties": {
                        "ID": point_id,
                        "Latitude": float(lat),
                        "Longitude": float(lon),
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [round(float(lon), 7), round(float(lat), 7)],
                    },
                })
        
        if len(valid_coords) == 0:
            raise HTTPException(
                status_code=400,
                detail="No valid coordinates found in shapefile"
            )
        
        # Create a filtered point IDs mapping for only valid coordinates
        valid_point_ids_mapping = {i: point_ids_mapping[idx] for i, idx in enumerate(valid_indices)}

        power_dataset_overrides = None
        if config.ENABLE_NASA_POWER:
            bounds = ShapefileProcessor.calculate_bounds(valid_coords)
            fetcher = get_fetcher()
            power_dataset_overrides = await fetcher.prepare_local_subsets(
                start_date=pd.to_datetime(start_date).date(),
                end_date=pd.to_datetime(end_date).date(),
                min_lat=bounds["lat_min"],
                max_lat=bounds["lat_max"],
                min_lon=bounds["lon_min"],
                max_lon=bounds["lon_max"],
            )
            logger.info("Using bounded local NASA POWER Zarr subsets for multi-point ICASA generation")
        
        # Open CHIRPS dataset
        ds = open_zarr()
        merger = WeatherDataMerger(ds, power_dataset_overrides=power_dataset_overrides)
        
        # Use bounded parallelism for multi-point generation.
        cpu_count = os.cpu_count() or 4
        max_workers = min(8, cpu_count)
        logger.info("Generating weather data package from local dataset subsets...")
        batch_generator = EnhancedIcasaBatchGenerator(max_workers=max_workers)
        
        files = await batch_generator.generate_batch_from_merger(
            coordinates=valid_coords,
            start_date=start_date,
            end_date=end_date,
            merger=merger,
            rain_source=rain_source,
            site_code=config.DEFAULT_SITE_CODE,
            selected_variables=selected_vars,
            point_ids_mapping=valid_point_ids_mapping,
        )
        
        # Create zip file
        from utils.async_processor import ZipFileBuilder
        
        source_desc = "CHIRPS + NASA POWER"
        if rain_source == "chirps":
            source_desc = "CHIRPS + NASA POWER (Rain: CHIRPS)"
        elif rain_source == "nasa_power":
            source_desc = "NASA POWER"
        
        additional_files = None
        if has_generated_ids:
            geojson_name = f"shapefile/{Path(filename).stem}_point_ids.geojson"
            geojson_payload = {
                "type": "FeatureCollection",
                "name": Path(filename).stem,
                "createdBy": "UFWeatherTool",
                "features": point_id_features,
            }
            additional_files = {
                geojson_name: json.dumps(geojson_payload, ensure_ascii=True, indent=2)
            }

        zip_content = ZipFileBuilder.create_zip_archive(
            files=files,
            include_readme=True,
            metadata={
                'start_date': start_date,
                'end_date': end_date,
                'total_points': len(valid_coords),
                'data_source': source_desc,
                'rain_source': rain_source
            },
            shapefile_path=None if has_generated_ids else spatial_path,
            additional_files=additional_files,
        )
        
        logger.info(f"Successfully generated package with {len(files)} files")
        
        # Create response
        zip_filename = f"weather_data_{start_date}_{end_date}.zip"
        
        return StreamingResponse(
            io.BytesIO(zip_content),
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={zip_filename}"}
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
    shapefile: UploadFile = File(..., description="Shapefile (.shp, .geojson, .json, or .zip)"),
    shapefile_shx: Optional[UploadFile] = File(None, description="Shapefile .shx companion file"),
    shapefile_dbf: Optional[UploadFile] = File(None, description="Shapefile .dbf companion file")
):
    """
    Validate a spatial file (shapefile, GeoJSON) and return coordinate information without processing.
    Useful for preview before downloading.
    
    Supports:
    - Shapefiles (.shp with optional .shx and .dbf files)
    - GeoJSON (.geojson, .json)
    - Zip archives containing spatial files
    """
    temp_dir = None
    
    try:
        # Read uploaded file
        shp_content = await shapefile.read()
        
        # Collect additional files if provided
        additional_files = {}
        if shapefile_shx:
            shx_content = await shapefile_shx.read()
            additional_files[shapefile_shx.filename or 'file.shx'] = shx_content
        
        if shapefile_dbf:
            dbf_content = await shapefile_dbf.read()
            additional_files[shapefile_dbf.filename or 'file.dbf'] = dbf_content
        
        # Save spatial file
        filename = shapefile.filename or 'spatial_file'
        spatial_path = ShapefileProcessor.save_uploaded_shapefile(
            uploaded_file=shp_content,
            filename=filename,
            additional_files=additional_files if additional_files else None
        )
        temp_dir = spatial_path.parent
        
        # Extract coordinates
        coordinates, point_ids_mapping, _ = ShapefileProcessor.extract_coordinates_and_ids_from_file(spatial_path)
        
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
        logger.error(f"Error validating spatial file: {e}")
        raise HTTPException(status_code=500, detail=f"Error validating spatial file: {str(e)}")
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
