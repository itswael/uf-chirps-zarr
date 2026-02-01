"""
TIFF to Zarr conversion for CHIRPS precipitation data.

Handles:
- GeoTIFF to xarray Dataset conversion
- Zarr store initialization
- Sequential time-dimension appending
- Metadata management
- Thread-safe write operations
"""

import threading
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

import numpy as np
import xarray as xr
import rioxarray as rxr
import zarr
from numcodecs import Blosc

from src.config import Config
from src.utils.logging import AuditLogger, setup_logger


class ZarrConversionError(Exception):
    """Exception raised during Zarr conversion operations."""
    pass


class TIFFToZarrConverter:
    """
    Converter for CHIRPS GeoTIFF files to Zarr format.
    
    Features:
    - Initialize Zarr stores with proper CF metadata
    - Convert GeoTIFF to xarray Dataset
    - Append data along time dimension
    - Thread-safe write operations
    - Immutable chunking and compression
    """
    
    # Thread lock for single-writer enforcement
    _write_lock = threading.Lock()
    
    def __init__(
        self,
        config: Config,
        audit_logger: Optional[AuditLogger] = None
    ):
        """
        Initialize the TIFF to Zarr converter.
        
        Args:
            config: Application configuration
            audit_logger: Optional audit logger
        """
        self.config = config
        self.logger = setup_logger(
            self.config.get_logger_name(__name__),
            log_dir=self.config.BASE_DIR / "logs"
        )
        self.audit_logger = audit_logger
    
    def tiff_to_dataset(
        self,
        tiff_path: Path,
        time_value: date
    ) -> xr.Dataset:
        """
        Convert a GeoTIFF file to an xarray Dataset.
        
        Args:
            tiff_path: Path to the GeoTIFF file
            time_value: Date for the time dimension
            
        Returns:
            xarray Dataset with time, latitude, longitude dimensions
            
        Raises:
            ZarrConversionError: If conversion fails
        """
        try:
            self.logger.debug(f"Converting {tiff_path} to xarray Dataset")
            
            # Read GeoTIFF with rioxarray
            da = rxr.open_rasterio(tiff_path, masked=True)
            
            # Extract spatial coordinates
            # CHIRPS uses lat/lon, rioxarray might have y/x
            if 'y' in da.dims and 'x' in da.dims:
                da = da.rename({'y': 'latitude', 'x': 'longitude'})
            
            # Remove band dimension (CHIRPS has single band)
            if 'band' in da.dims:
                da = da.squeeze('band', drop=True)
            
            # Add time dimension
            da = da.expand_dims(time=[np.datetime64(time_value)])
            
            # Rename data array to 'precipitation'
            da.name = 'precipitation'
            
            # Convert to Dataset
            ds = da.to_dataset()
            
            # Set fill value
            ds['precipitation'].encoding['_FillValue'] = self.config.PRECIPITATION_FILL_VALUE
            
            # Add variable attributes
            ds['precipitation'].attrs.update({
                'long_name': 'Daily precipitation',
                'standard_name': 'precipitation_amount',
                'units': 'mm/day',
                'valid_min': self.config.PRECIPITATION_VALID_MIN,
                'grid_mapping': 'crs'
            })
            
            # Add coordinate attributes
            ds['time'].attrs.update({
                'long_name': 'Time',
                'standard_name': 'time',
                'axis': 'T'
            })
            
            ds['latitude'].attrs.update({
                'long_name': 'Latitude',
                'standard_name': 'latitude',
                'units': 'degrees_north',
                'axis': 'Y'
            })
            
            ds['longitude'].attrs.update({
                'long_name': 'Longitude',
                'standard_name': 'longitude',
                'units': 'degrees_east',
                'axis': 'X'
            })
            
            # Add CRS information
            ds.attrs['crs'] = 'EPSG:4326'
            
            self.logger.debug(
                f"Converted to Dataset: {tuple(ds.dims.items())}, "
                f"variables: {list(ds.data_vars)}"
            )
            
            return ds
            
        except Exception as e:
            raise ZarrConversionError(f"Failed to convert {tiff_path} to Dataset: {e}")
    
    def initialize_zarr_store(
        self,
        first_dataset: xr.Dataset,
        zarr_path: Path,
        start_date: date,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Initialize a new Zarr store with the first time slice.
        
        Args:
            first_dataset: First xarray Dataset to initialize the store
            zarr_path: Path for the Zarr store
            start_date: Start date for time_coverage_start
            metadata: Optional additional metadata
            
        Raises:
            ZarrConversionError: If initialization fails
        """
        if zarr_path.exists():
            raise ZarrConversionError(
                f"Zarr store already exists: {zarr_path}. "
                "Delete it first or use append mode."
            )
        
        try:
            self.logger.info(f"Initializing Zarr store: {zarr_path}")
            
            # Load custom metadata from config
            custom_metadata = self.config.load_metadata_config()
            
            # Prepare dataset-level metadata
            ds_metadata = {
                'title': custom_metadata.get('title', 'CHIRPS Daily Precipitation Data'),
                'institution': custom_metadata.get(
                    'institution',
                    'University of Florida - Climate Hazards Center'
                ),
                'source': custom_metadata.get('source', 'CHIRPS version 3.0'),
                'references': custom_metadata.get('references', 'Funk et al. 2015, Scientific Data'),
                'comment': custom_metadata.get(
                    'comment',
                    'Climate Hazards Group InfraRed Precipitation with Station data'
                ),
                'Conventions': 'CF-1.8',
                'product_version': '1.0',
                'time_coverage_start': start_date.isoformat(),
                'time_coverage_end': start_date.isoformat(),  # Will be updated
                'date_created': datetime.now(timezone.utc).isoformat(),
                'history': f'Created on {datetime.now(timezone.utc).isoformat()}Z',
                'geospatial_lat_min': float(first_dataset.latitude.min()),
                'geospatial_lat_max': float(first_dataset.latitude.max()),
                'geospatial_lon_min': float(first_dataset.longitude.min()),
                'geospatial_lon_max': float(first_dataset.longitude.max()),
                'geospatial_lat_resolution': 0.05,
                'geospatial_lon_resolution': 0.05,
                'geospatial_lat_units': 'degrees_north',
                'geospatial_lon_units': 'degrees_east'
            }
            
            # Merge with any provided metadata
            if metadata:
                ds_metadata.update(metadata)
            
            # Apply metadata to dataset
            first_dataset.attrs.update(ds_metadata)
            
            # Define encoding with chunking (compression handled by zarr defaults)
            encoding = {
                'precipitation': {
                    'chunks': (
                        self.config.ZARR_CHUNK_TIME,
                        self.config.ZARR_CHUNK_LAT,
                        self.config.ZARR_CHUNK_LON
                    ),
                    '_FillValue': self.config.PRECIPITATION_FILL_VALUE,
                    'dtype': 'float32'
                },
                'time': {
                    'units': 'days since 1970-01-01',
                    'calendar': 'gregorian',
                    'dtype': 'int32'
                },
                'latitude': {'dtype': 'float32'},
                'longitude': {'dtype': 'float32'}
            }
            
            # Create parent directory
            zarr_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to Zarr
            first_dataset.to_zarr(
                zarr_path,
                mode='w',
                encoding=encoding,
                consolidated=True
            )
            
            dimensions = {
                'time': int(first_dataset.sizes['time']),
                'latitude': int(first_dataset.sizes['latitude']),
                'longitude': int(first_dataset.sizes['longitude'])
            }
            
            chunks = {
                'time': self.config.ZARR_CHUNK_TIME,
                'latitude': self.config.ZARR_CHUNK_LAT,
                'longitude': self.config.ZARR_CHUNK_LON
            }
            
            self.logger.info(
                f"Zarr store initialized: {zarr_path}, "
                f"dimensions={dimensions}, chunks={chunks}"
            )
            
            # Audit log
            if self.audit_logger:
                self.audit_logger.log_zarr_init(
                    str(zarr_path),
                    dimensions,
                    chunks,
                    start_date.isoformat(),
                    ds_metadata
                )
            
        except Exception as e:
            raise ZarrConversionError(f"Failed to initialize Zarr store: {e}")
    
    def append_to_zarr(
        self,
        dataset: xr.Dataset,
        zarr_path: Path,
        time_value: date
    ) -> int:
        """
        Append a dataset to an existing Zarr store along the time dimension.
        
        This operation is thread-safe (single-writer lock).
        
        Args:
            dataset: xarray Dataset to append
            zarr_path: Path to the Zarr store
            time_value: Date being appended
            
        Returns:
            Time index where data was appended
            
        Raises:
            ZarrConversionError: If append fails
        """
        if not zarr_path.exists():
            raise ZarrConversionError(
                f"Zarr store does not exist: {zarr_path}. "
                "Initialize it first."
            )
        
        # Enforce single-writer using thread lock
        with self._write_lock:
            try:
                start_time = time.time()
                
                self.logger.debug(f"Appending {time_value} to {zarr_path}")
                
                # Open existing Zarr store
                existing_ds = xr.open_zarr(zarr_path)
                current_time_size = existing_ds.sizes['time']
                existing_ds.close()
                
                # Remove encoding-related attributes that may conflict
                # xarray adds these during rioxarray operations
                attrs_to_remove = ['add_offset', 'scale_factor', '_FillValue']
                for var_name in dataset.data_vars:
                    for attr in attrs_to_remove:
                        if attr in dataset[var_name].attrs:
                            del dataset[var_name].attrs[attr]
                
                # Append to Zarr (no encoding needed - uses existing configuration)
                dataset.to_zarr(
                    zarr_path,
                    mode='a',
                    append_dim='time',
                    consolidated=True
                )
                
                duration = time.time() - start_time
                
                self.logger.info(
                    f"Appended {time_value} to Zarr at index {current_time_size} "
                    f"in {duration:.2f}s"
                )
                
                # Audit log
                if self.audit_logger:
                    self.audit_logger.log_zarr_append(
                        time_value.isoformat(),
                        str(zarr_path),
                        current_time_size,
                        duration
                    )
                
                return current_time_size
                
            except Exception as e:
                raise ZarrConversionError(
                    f"Failed to append {time_value} to Zarr: {e}"
                )
    
    def update_metadata(
        self,
        zarr_path: Path,
        updates: Dict[str, Any]
    ) -> None:
        """
        Update metadata in an existing Zarr store.
        
        Args:
            zarr_path: Path to the Zarr store
            updates: Dictionary of metadata updates
            
        Raises:
            ZarrConversionError: If update fails
        """
        try:
            self.logger.debug(f"Updating metadata for {zarr_path}")
            
            # Open Zarr store
            store = zarr.open(str(zarr_path), mode='r+')
            
            # Update attributes
            for key, value in updates.items():
                store.attrs[key] = value
            
            # Consolidate metadata
            zarr.consolidate_metadata(zarr_path)
            
            self.logger.info(f"Updated metadata for {zarr_path}: {list(updates.keys())}")
            
        except Exception as e:
            raise ZarrConversionError(f"Failed to update metadata: {e}")
    
    def finalize_zarr_store(
        self,
        zarr_path: Path,
        end_date: date
    ) -> None:
        """
        Finalize a Zarr store after bootstrap completion.
        
        Updates time_coverage_end and adds completion metadata.
        
        Args:
            zarr_path: Path to the Zarr store
            end_date: Final date in the store
            
        Raises:
            ZarrConversionError: If finalization fails
        """
        updates = {
            'time_coverage_end': end_date.isoformat(),
            'date_modified': datetime.now(timezone.utc).isoformat(),
            'bootstrap_complete': True
        }
        
        self.update_metadata(zarr_path, updates)
        self.logger.info(f"Finalized Zarr store: {zarr_path}, end_date={end_date}")
    
    def get_zarr_info(self, zarr_path: Path) -> Dict[str, Any]:
        """
        Get information about a Zarr store.
        
        Args:
            zarr_path: Path to the Zarr store
            
        Returns:
            Dictionary with store information
        """
        if not zarr_path.exists():
            return {"exists": False}
        
        try:
            ds = xr.open_zarr(zarr_path)
            
            info = {
                "exists": True,
                "dimensions": dict(ds.sizes),
                "variables": list(ds.data_vars),
                "coordinates": list(ds.coords),
                "time_coverage_start": ds.attrs.get('time_coverage_start'),
                "time_coverage_end": ds.attrs.get('time_coverage_end'),
                "bootstrap_complete": ds.attrs.get('bootstrap_complete', False)
            }
            
            ds.close()
            return info
            
        except Exception as e:
            return {"exists": True, "error": str(e)}
