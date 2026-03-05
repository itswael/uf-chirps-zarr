"""
NASA POWER S3 Data Fetcher
Fetches daily meteorological and solar data from NASA POWER S3 Zarr stores
"""
import asyncio
import logging
import ssl
from datetime import datetime, date
from typing import Optional, Dict, Any, List
import pandas as pd
import xarray as xr
import fsspec

from .nasa_power_config import nasa_power_config
from ..config import Config

logger = logging.getLogger(__name__)


class NasaPowerS3Fetcher:
    """Fetch data from NASA POWER S3 Zarr stores"""
    
    def __init__(self):
        """Initialize the fetcher"""
        self._syn1_ds: Optional[xr.Dataset] = None
        self._merra2_ds: Optional[xr.Dataset] = None
        self._datasets_loaded = False
    
    def _open_power_zarr(self, zarr_url: str) -> xr.Dataset:
        """
        Open a NASA POWER Zarr dataset from S3.
        
        Args:
            zarr_url: HTTPS URL to Zarr store
            
        Returns:
            Opened xarray Dataset
        """
        try:
            # Configure SSL settings for fsspec
            client_kwargs = {}
            
            if not Config.NASA_POWER_VERIFY_SSL:
                logger.warning(
                    "SSL certificate verification is DISABLED for NASA POWER S3 access. "
                    "This is not recommended for production use."
                )
                # Create SSL context that doesn't verify certificates
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                client_kwargs['client_kwargs'] = {'verify': False}
            elif Config.NASA_POWER_SSL_CERT_PATH:
                logger.info(f"Using custom SSL certificate: {Config.NASA_POWER_SSL_CERT_PATH}")
                client_kwargs['client_kwargs'] = {'verify': Config.NASA_POWER_SSL_CERT_PATH}
            
            # Open Zarr store with configured SSL settings
            store = fsspec.get_mapper(zarr_url, **client_kwargs)
            ds = xr.open_zarr(store, consolidated=True)
            logger.info(f"Successfully opened Zarr store: {zarr_url}")
            return ds
        except Exception as e:
            logger.error(f"Error opening Zarr store {zarr_url}: {e}")
            raise
    
    async def load_datasets(self):
        """
        Load NASA POWER datasets asynchronously.
        This should be called once at startup to cache the datasets.
        """
        if self._datasets_loaded:
            return
        
        logger.info("Loading NASA POWER datasets from S3...")
        
        try:
            # Load datasets in parallel using thread pool
            loop = asyncio.get_event_loop()
            
            syn1_task = loop.run_in_executor(
                None,
                self._open_power_zarr,
                nasa_power_config.SYN1DAILY_ZARR_URL
            )
            
            merra2_task = loop.run_in_executor(
                None,
                self._open_power_zarr,
                nasa_power_config.MERRA2DAILY_ZARR_URL
            )
            
            self._syn1_ds, self._merra2_ds = await asyncio.gather(syn1_task, merra2_task)
            self._datasets_loaded = True
            
            logger.info("NASA POWER datasets loaded successfully")
            
        except Exception as e:
            logger.error(f"Error loading NASA POWER datasets: {e}")
            raise
    
    def _slice_point(
        self,
        ds: xr.Dataset,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        variables: List[str]
    ) -> xr.Dataset:
        """
        Extract data for a specific point and date range.
        
        Args:
            ds: Xarray dataset
            latitude: Latitude
            longitude: Longitude
            start_date: Start date
            end_date: End date
            variables: List of variables to extract
            
        Returns:
            Sliced xarray Dataset
        """
        # Filter for available variables
        avail_vars = [v for v in variables if v in ds.data_vars]
        
        if not avail_vars:
            raise KeyError(
                f"None of the requested variables are present. "
                f"Requested: {variables}, Available: {list(ds.data_vars)[:10]}"
            )
        
        # Select spatial point (nearest neighbor due to 0.5° resolution)
        sub = ds[avail_vars].sel(
            lat=latitude,
            lon=longitude,
            method="nearest"
        )
        
        # Select time range
        sub = sub.sel(
            time=slice(
                datetime.combine(start_date, datetime.min.time()),
                datetime.combine(end_date, datetime.min.time())
            )
        )
        
        return sub
    
    async def fetch_nasa_power_data(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        include_solar: bool = True,
        include_met: bool = True
    ) -> pd.DataFrame:
        """
        Fetch NASA POWER data for a specific location and date range.
        
        Args:
            latitude: Latitude of the point
            longitude: Longitude of the point
            start_date: Start date
            end_date: End date
            include_solar: Include solar radiation data
            include_met: Include meteorological data
            
        Returns:
            DataFrame with time series data
        """
        # Ensure datasets are loaded
        if not self._datasets_loaded:
            await self.load_datasets()
        
        try:
            df = None
            
            # Fetch meteorological data from MERRA-2
            if include_met and self._merra2_ds is not None:
                loop = asyncio.get_event_loop()
                sub_met = await loop.run_in_executor(
                    None,
                    self._slice_point,
                    self._merra2_ds,
                    latitude,
                    longitude,
                    start_date,
                    end_date,
                    nasa_power_config.MET_VARS
                )
                
                # Convert to DataFrame and rename variables
                df_met = sub_met.to_dataframe().reset_index()
                df_met = df_met.rename(columns=nasa_power_config.RENAME_MET_VARS)
                df = df_met
            
            # Fetch solar data from SYN1deg
            if include_solar and self._syn1_ds is not None:
                loop = asyncio.get_event_loop()
                sub_sol = await loop.run_in_executor(
                    None,
                    self._slice_point,
                    self._syn1_ds,
                    latitude,
                    longitude,
                    start_date,
                    end_date,
                    nasa_power_config.SOLAR_VARS
                )
                
                # Convert to DataFrame and rename variables
                df_sol = sub_sol.to_dataframe().reset_index()
                df_sol = df_sol.rename(columns=nasa_power_config.RENAME_SOLAR_VARS)
                
                # Convert W/m^2 (mean power) to MJ/m^2/day
                df_sol["SRAD"] = df_sol["SRAD_WM2"].astype(float) * 0.0864
                df_sol = df_sol[["time", "SRAD"]]
                
                # Merge with meteorological data if available
                if df is None:
                    df = df_sol
                else:
                    df = pd.merge(df, df_sol, on="time", how="inner")
            
            if df is None:
                raise ValueError("No data sources selected")
            
            # Rename additional variables for ICASA compatibility
            rename_map = {
                'T2MDEW': 'TDEW',
                'WS2M': 'WIND'
            }
            df = df.rename(columns=rename_map)
            
            # Round values to 1 decimal place
            numeric_cols = df.select_dtypes(include=['float64', 'float32']).columns
            df[numeric_cols] = df[numeric_cols].round(1)
            
            logger.info(
                f"Fetched NASA POWER data: {len(df)} days, "
                f"variables: {[c for c in df.columns if c not in ['time', 'lat', 'lon']]}"
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching NASA POWER data: {e}")
            raise
    
    async def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about NASA POWER datasets.
        
        Returns:
            Dictionary with dataset metadata
        """
        if not self._datasets_loaded:
            await self.load_datasets()
        
        metadata = {
            'solar': {},
            'meteorological': {}
        }
        
        if self._syn1_ds is not None:
            time_values = self._syn1_ds.time.values
            lat_values = self._syn1_ds.lat.values
            lon_values = self._syn1_ds.lon.values
            
            metadata['solar'] = {
                'variables': list(self._syn1_ds.data_vars),
                'time_range': {
                    'start': str(time_values[0]),
                    'end': str(time_values[-1]),
                    'total_days': len(time_values)
                },
                'spatial_extent': {
                    'latitude': {
                        'min': float(lat_values.min()),
                        'max': float(lat_values.max()),
                        'resolution': nasa_power_config.NASA_POWER_RESOLUTION
                    },
                    'longitude': {
                        'min': float(lon_values.min()),
                        'max': float(lon_values.max()),
                        'resolution': nasa_power_config.NASA_POWER_RESOLUTION
                    }
                }
            }
        
        if self._merra2_ds is not None:
            time_values = self._merra2_ds.time.values
            lat_values = self._merra2_ds.lat.values
            lon_values = self._merra2_ds.lon.values
            
            metadata['meteorological'] = {
                'variables': list(self._merra2_ds.data_vars),
                'time_range': {
                    'start': str(time_values[0]),
                    'end': str(time_values[-1]),
                    'total_days': len(time_values)
                },
                'spatial_extent': {
                    'latitude': {
                        'min': float(lat_values.min()),
                        'max': float(lat_values.max()),
                        'resolution': nasa_power_config.NASA_POWER_RESOLUTION
                    },
                    'longitude': {
                        'min': float(lon_values.min()),
                        'max': float(lon_values.max()),
                        'resolution': nasa_power_config.NASA_POWER_RESOLUTION
                    }
                }
            }
        
        return metadata
    
    def close(self):
        """Close the datasets"""
        if self._syn1_ds is not None:
            self._syn1_ds.close()
            self._syn1_ds = None
        
        if self._merra2_ds is not None:
            self._merra2_ds.close()
            self._merra2_ds = None
        
        self._datasets_loaded = False


# Global fetcher instance
_global_fetcher: Optional[NasaPowerS3Fetcher] = None


def get_fetcher() -> NasaPowerS3Fetcher:
    """
    Get or create the global NASA POWER fetcher instance.
    
    Returns:
        NasaPowerS3Fetcher instance
    """
    global _global_fetcher
    if _global_fetcher is None:
        _global_fetcher = NasaPowerS3Fetcher()
    return _global_fetcher
