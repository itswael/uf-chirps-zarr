"""
NASA POWER S3 Data Fetcher
Fetches daily meteorological and solar data from NASA POWER S3 Zarr stores
"""
import asyncio
from collections import OrderedDict
from pathlib import Path
import logging
import os
import hashlib
import shutil
import tempfile
from datetime import datetime, date
from typing import Optional, Dict, Any, List
import pandas as pd
import xarray as xr
import fsspec
import certifi

from .nasa_power_config import nasa_power_config
try:
    from ..config import Config
except ImportError:
    from config import Config

logger = logging.getLogger(__name__)


class NasaPowerS3Fetcher:
    """Fetch data from NASA POWER S3 Zarr stores"""
    
    def __init__(self):
        """Initialize the fetcher"""
        self._syn1_legacy_ds: Optional[xr.Dataset] = None
        self._syn1_ds: Optional[xr.Dataset] = None
        self._merra2_ds: Optional[xr.Dataset] = None
        self._datasets_loaded = False
        self._cache_lock = asyncio.Lock()
        self._date_slice_cache: Dict[str, OrderedDict[str, xr.Dataset]] = {
            "syn1_legacy": OrderedDict(),
            "syn1": OrderedDict(),
            "merra2": OrderedDict(),
        }
        self._local_subset_cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self._date_slice_cache_limit = 8
        self._local_subset_cache_limit = 4
        self._local_cache_root = Path(tempfile.gettempdir()) / "uf-chirps-zarr" / "nasa-power-subsets"
        self._local_cache_root.mkdir(parents=True, exist_ok=True)
        # Track dataset time ranges for validation
        self._syn1_legacy_time_range: Optional[tuple] = None
        self._syn1_time_range: Optional[tuple] = None
        self._merra2_time_range: Optional[tuple] = None
    
    def _open_power_zarr(self, zarr_url: str) -> xr.Dataset:
        """
        Open a NASA POWER Zarr dataset from S3.
        
        Args:
            zarr_url: HTTPS URL to Zarr store
            
        Returns:
            Opened xarray Dataset
        """
        try:
            # Configure SSL settings for HTTP access (thread-safe)
            if not Config.NASA_POWER_VERIFY_SSL:
                logger.warning(
                    "SSL certificate verification is DISABLED for NASA POWER S3 access. "
                    "This is not recommended for production use."
                )
                os.environ["PYTHONHTTPSVERIFY"] = "0"
            elif Config.NASA_POWER_SSL_CERT_PATH:
                logger.info(f"Using custom SSL certificate: {Config.NASA_POWER_SSL_CERT_PATH}")
                os.environ["SSL_CERT_FILE"] = Config.NASA_POWER_SSL_CERT_PATH
                os.environ["REQUESTS_CA_BUNDLE"] = Config.NASA_POWER_SSL_CERT_PATH
            else:
                # Use certifi CA bundle by default (helps on macOS environments
                # with incomplete system certificate chains)
                cert_path = certifi.where()
                logger.info(f"Using certifi CA bundle for SSL verification: {cert_path}")
                os.environ["SSL_CERT_FILE"] = cert_path
                os.environ["REQUESTS_CA_BUNDLE"] = cert_path
            
            # Open Zarr store with configured SSL settings
            store = fsspec.get_mapper(zarr_url)
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
            
            syn1_legacy_task = loop.run_in_executor(
                None,
                self._open_power_zarr,
                nasa_power_config.LEGACY_SYN1DAILY_ZARR_URL
            )

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
            
            self._syn1_legacy_ds, self._syn1_ds, self._merra2_ds = await asyncio.gather(
                syn1_legacy_task,
                syn1_task,
                merra2_task,
            )
            self._datasets_loaded = True
            
            # Cache time ranges for validation
            if self._syn1_legacy_ds is not None:
                time_values = pd.to_datetime(self._syn1_legacy_ds.time.values)
                self._syn1_legacy_time_range = (time_values[0], time_values[-1])
                logger.info(
                    f"Legacy SYN1deg time range: {self._syn1_legacy_time_range[0]} to {self._syn1_legacy_time_range[1]}"
                )

            if self._syn1_ds is not None:
                time_values = pd.to_datetime(self._syn1_ds.time.values)
                self._syn1_time_range = (time_values[0], time_values[-1])
                logger.info(f"SYN1deg time range: {self._syn1_time_range[0]} to {self._syn1_time_range[1]}")
            
            if self._merra2_ds is not None:
                time_values = pd.to_datetime(self._merra2_ds.time.values)
                self._merra2_time_range = (time_values[0], time_values[-1])
                logger.info(f"MERRA-2 time range: {self._merra2_time_range[0]} to {self._merra2_time_range[1]}")
            
            logger.info("NASA POWER datasets loaded successfully")
            
        except Exception as e:
            logger.error(f"Error loading NASA POWER datasets: {e}")
            raise

    @staticmethod
    def _format_date_cache_key(start_date: date, end_date: date) -> str:
        """Create a stable cache key for a date range."""
        return f"{start_date.isoformat()}_{end_date.isoformat()}"

    @staticmethod
    def _coordinate_slice(values, min_value: float, max_value: float) -> slice:
        """Build an inclusive slice that respects coordinate ordering."""
        lower = min(min_value, max_value)
        upper = max(min_value, max_value)

        if len(values) < 2:
            return slice(lower, upper)

        first = float(values[0])
        last = float(values[-1])
        if first <= last:
            return slice(lower, upper)
        return slice(upper, lower)

    def _check_date_range_available(
        self,
        start_date: date,
        end_date: date,
        dataset_name: str
    ) -> bool:
        """
        Check if the requested date range has data in the specified dataset.
        
        Args:
            start_date: Start date
            end_date: End date
            dataset_name: Either 'syn1' or 'merra2'
            
        Returns:
            True if data is available for any part of the date range
        """
        if dataset_name == "syn1_legacy" and self._syn1_legacy_time_range:
            data_start, data_end = self._syn1_legacy_time_range
        elif dataset_name == "syn1" and self._syn1_time_range:
            data_start, data_end = self._syn1_time_range
        elif dataset_name == "merra2" and self._merra2_time_range:
            data_start, data_end = self._merra2_time_range
        else:
            # Dataset not loaded yet
            return True
        
        # Convert dates to timestamps for comparison
        req_start = pd.Timestamp(start_date)
        req_end = pd.Timestamp(end_date)
        
        # Check if requested range overlaps with available data
        overlaps = (req_start <= data_end) and (req_end >= data_start)
        
        if not overlaps:
            logger.warning(
                f"{dataset_name.upper()} dataset has no data for requested range "
                f"{start_date} to {end_date} (available: {data_start.date()} to {data_end.date()})"
            )
        
        return overlaps

    @staticmethod
    def _clip_date_range(start_date: date, end_date: date, range_start: date, range_end: date) -> Optional[tuple]:
        """Clip a requested date range to a dataset range."""
        clipped_start = max(start_date, range_start)
        clipped_end = min(end_date, range_end)
        if clipped_start > clipped_end:
            return None
        return clipped_start, clipped_end

    def _get_solar_segments(
        self,
        start_date: date,
        end_date: date,
        source_datasets: Dict[str, xr.Dataset]
    ) -> List[Dict[str, Any]]:
        """Build the solar dataset segments needed for the requested date range."""
        segments: List[Dict[str, Any]] = []

        legacy_range = self._clip_date_range(
            start_date,
            end_date,
            nasa_power_config.SYN1_LEGACY_START_DATE,
            nasa_power_config.SYN1_LEGACY_END_DATE,
        )
        if legacy_range is not None and source_datasets.get("syn1_legacy") is not None:
            if self._syn1_legacy_time_range is not None:
                legacy_range = self._clip_date_range(
                    legacy_range[0],
                    legacy_range[1],
                    self._syn1_legacy_time_range[0].date(),
                    self._syn1_legacy_time_range[1].date(),
                )
            if legacy_range is not None:
                segments.append({
                    "dataset_name": "syn1_legacy",
                    "dataset": source_datasets["syn1_legacy"],
                    "start_date": legacy_range[0],
                    "end_date": legacy_range[1],
                })

        current_range = self._clip_date_range(
            start_date,
            end_date,
            nasa_power_config.SYN1_CURRENT_START_DATE,
            end_date,
        )
        if current_range is not None and source_datasets.get("syn1") is not None:
            if self._syn1_time_range is not None:
                current_range = self._clip_date_range(
                    current_range[0],
                    current_range[1],
                    self._syn1_time_range[0].date(),
                    self._syn1_time_range[1].date(),
                )
            if current_range is not None:
                segments.append({
                    "dataset_name": "syn1",
                    "dataset": source_datasets["syn1"],
                    "start_date": current_range[0],
                    "end_date": current_range[1],
                })

        return segments

    def _slice_date_range(
        self,
        ds: xr.Dataset,
        start_date: date,
        end_date: date,
        variables: List[str]
    ) -> xr.Dataset:
        """Create a lazily sliced dataset for the requested date range."""
        return ds[variables].sel(
            time=slice(
                datetime.combine(start_date, datetime.min.time()),
                datetime.combine(end_date, datetime.min.time())
            )
        )

    def _materialize_local_subset(
        self,
        ds: xr.Dataset,
        local_path: Path,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float
    ) -> xr.Dataset:
        """Write a bounded local Zarr subset and reopen it for repeated point access."""
        if local_path.exists():
            return xr.open_zarr(local_path, consolidated=True)

        lat_slice = self._coordinate_slice(ds.lat.values, min_lat, max_lat)
        lon_slice = self._coordinate_slice(ds.lon.values, min_lon, max_lon)
        subset = ds.sel(lat=lat_slice, lon=lon_slice)

        if subset.sizes.get("lat", 0) == 0 or subset.sizes.get("lon", 0) == 0:
            raise ValueError(
                "No NASA POWER grid cells found for the requested bounds"
            )

        for var_name in subset.variables:
            subset[var_name].encoding = {}

        subset = subset.chunk({"time": -1, "lat": 10, "lon": 10})

        try:
            subset.to_zarr(local_path, mode="w", consolidated=True)
            return xr.open_zarr(local_path, consolidated=True)
        except Exception:
            shutil.rmtree(local_path, ignore_errors=True)
            raise

    def _trim_date_slice_cache(self, dataset_name: str) -> None:
        """Keep the in-memory date-slice cache bounded."""
        cache = self._date_slice_cache[dataset_name]
        while len(cache) > self._date_slice_cache_limit:
            cache.popitem(last=False)

    def _trim_local_subset_cache(self) -> None:
        """Keep the in-memory local subset cache bounded."""
        while len(self._local_subset_cache) > self._local_subset_cache_limit:
            self._local_subset_cache.popitem(last=False)

    async def prepare_date_range_cache(
        self,
        start_date: date,
        end_date: date
    ) -> Dict[str, xr.Dataset]:
        """Warm and return date-scoped NASA POWER datasets."""
        if not self._datasets_loaded:
            await self.load_datasets()

        cache_key = self._format_date_cache_key(start_date, end_date)

        async with self._cache_lock:
            dataset_configs = (
                ("merra2", self._merra2_ds, nasa_power_config.MET_VARS),
                ("syn1_legacy", self._syn1_legacy_ds, nasa_power_config.SOLAR_VARS),
                ("syn1", self._syn1_ds, nasa_power_config.SOLAR_VARS),
            )

            for dataset_name, ds, variables in dataset_configs:
                if ds is None:
                    continue

                cache = self._date_slice_cache[dataset_name]
                if cache_key not in cache:
                    sliced = self._slice_date_range(ds, start_date, end_date, variables)
                    if sliced.sizes.get("time", 0) > 0:
                        cache[cache_key] = sliced
                        self._trim_date_slice_cache(dataset_name)
                if cache_key in cache:
                    cache.move_to_end(cache_key)

            return {
                dataset_name: cache[cache_key]
                for dataset_name, cache in self._date_slice_cache.items()
                if cache_key in cache
            }

    async def prepare_local_subsets(
        self,
        start_date: date,
        end_date: date,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float
    ) -> Dict[str, xr.Dataset]:
        """Create or reuse local Zarr subsets for a bounded multi-point request."""
        date_datasets = await self.prepare_date_range_cache(start_date, end_date)

        bounds_key = (
            f"{start_date.isoformat()}_{end_date.isoformat()}_"
            f"{min_lat:.4f}_{max_lat:.4f}_{min_lon:.4f}_{max_lon:.4f}"
        )
        cache_key = hashlib.sha256(bounds_key.encode("utf-8")).hexdigest()[:16]

        async with self._cache_lock:
            if cache_key in self._local_subset_cache:
                self._local_subset_cache.move_to_end(cache_key)
                return self._local_subset_cache[cache_key]["datasets"]

            subset_dir = self._local_cache_root / cache_key
            subset_dir.mkdir(parents=True, exist_ok=True)

            datasets = {
                dataset_name: self._materialize_local_subset(
                    ds=dataset,
                    local_path=subset_dir / f"{dataset_name}.zarr",
                    min_lat=min_lat,
                    max_lat=max_lat,
                    min_lon=min_lon,
                    max_lon=max_lon,
                )
                for dataset_name, dataset in date_datasets.items()
            }

            self._local_subset_cache[cache_key] = {
                "path": subset_dir,
                "datasets": datasets,
            }
            self._trim_local_subset_cache()

            logger.info(
                "Prepared local NASA POWER subset cache for %s to %s within lat %.4f..%.4f, lon %.4f..%.4f",
                start_date,
                end_date,
                min_lat,
                max_lat,
                min_lon,
                max_lon,
            )

            return datasets
    
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
            Sliced xarray Dataset (may be empty if no data for date range)
            
        Raises:
            KeyError: If none of the requested variables exist in the dataset
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
        
        # Check if we got any data
        if sub.sizes.get("time", 0) == 0:
            logger.debug(
                f"No data retrieved for point ({latitude}, {longitude}) "
                f"on {start_date} to {end_date} from dataset with vars {avail_vars}"
            )
        
        return sub

    async def fetch_nasa_power_data(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        include_solar: bool = True,
        include_met: bool = True,
        dataset_overrides: Optional[Dict[str, xr.Dataset]] = None
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
            dataset_overrides: Optional pre-sliced datasets to use instead of fetching
            
        Returns:
            DataFrame with time series data
            
        Raises:
            ValueError: If no data is available for the requested date range
        """
        # Ensure datasets are loaded
        if not self._datasets_loaded:
            await self.load_datasets()
        
        try:
            df = None
            source_datasets = dataset_overrides or await self.prepare_date_range_cache(start_date, end_date)
            
            # Check date range availability
            merra2_available = self._check_date_range_available(start_date, end_date, "merra2")
            
            # Fetch meteorological data from MERRA-2
            if include_met and source_datasets.get("merra2") is not None and merra2_available:
                loop = asyncio.get_event_loop()
                sub_met = await loop.run_in_executor(
                    None,
                    self._slice_point,
                    source_datasets["merra2"],
                    latitude,
                    longitude,
                    start_date,
                    end_date,
                    nasa_power_config.MET_VARS
                )
                
                # Only process if we got data
                if sub_met.sizes.get("time", 0) > 0:
                    # Convert to DataFrame and rename variables
                    df_met = sub_met.to_dataframe().reset_index()
                    df_met = df_met.rename(columns=nasa_power_config.RENAME_MET_VARS)
                    df = df_met
                    logger.debug(f"Retrieved {len(df)} days of meteorological data from MERRA-2")
                else:
                    logger.warning(
                        f"MERRA-2 returned no data for ({latitude}, {longitude}) "
                        f"on {start_date} to {end_date}"
                    )
            elif include_met and not merra2_available:
                logger.warning(
                    f"MERRA-2 meteorological data not available for {start_date} to {end_date}. "
                    f"MERRA-2 only covers from {self._merra2_time_range[0].date() if self._merra2_time_range else 'unknown'}"
                )
            
            solar_segments = self._get_solar_segments(start_date, end_date, source_datasets) if include_solar else []
            solar_frames: List[pd.DataFrame] = []

            if include_solar and not solar_segments:
                logger.warning(
                    f"Solar radiation data not available for {start_date} to {end_date}. "
                    f"Legacy SYN1deg: {nasa_power_config.SYN1_LEGACY_START_DATE} to {nasa_power_config.SYN1_LEGACY_END_DATE}, "
                    f"current SYN1deg: {nasa_power_config.SYN1_CURRENT_START_DATE} onward"
                )

            for segment in solar_segments:
                loop = asyncio.get_event_loop()
                sub_sol = await loop.run_in_executor(
                    None,
                    self._slice_point,
                    segment["dataset"],
                    latitude,
                    longitude,
                    segment["start_date"],
                    segment["end_date"],
                    nasa_power_config.SOLAR_VARS
                )

                if sub_sol.sizes.get("time", 0) > 0:
                    df_sol = sub_sol.to_dataframe().reset_index()
                    df_sol = df_sol.rename(columns=nasa_power_config.RENAME_SOLAR_VARS)
                    df_sol["SRAD"] = df_sol["SRAD_WM2"].astype(float) * 0.0864
                    solar_frames.append(df_sol[["time", "SRAD"]])
                    logger.debug(
                        f"Retrieved solar radiation data from {segment['dataset_name']} for "
                        f"{segment['start_date']} to {segment['end_date']}"
                    )
                else:
                    logger.warning(
                        f"{segment['dataset_name']} returned no solar data for ({latitude}, {longitude}) "
                        f"on {segment['start_date']} to {segment['end_date']}"
                    )

            if solar_frames:
                df_sol = pd.concat(solar_frames, ignore_index=True)
                df_sol = df_sol.drop_duplicates(subset=["time"]).sort_values("time")

                if df is None:
                    df = df_sol
                else:
                    df = pd.merge(df, df_sol, on="time", how="left")

            if include_solar and df is not None:
                full_time_index = pd.DataFrame({"time": pd.date_range(start=start_date, end=end_date, freq="D")})
                df = pd.merge(full_time_index, df, on="time", how="left")

            # S3-only behavior: if solar is requested but unavailable/missing, set SRAD to -99
            if include_solar:
                if df is None:
                    fallback_time = pd.date_range(start=start_date, end=end_date, freq="D")
                    df = pd.DataFrame({"time": fallback_time, "SRAD": -99.0})
                elif "SRAD" not in df.columns:
                    df["SRAD"] = -99.0
                else:
                    df["SRAD"] = df["SRAD"].fillna(-99.0)
            
            if df is None:
                error_msg = (
                    f"No NASA POWER data available for ({latitude}, {longitude}) "
                    f"from {start_date} to {end_date}. "
                )
                solar_available = bool(solar_segments)
                if not merra2_available and not solar_available:
                    error_msg += (
                        f"Requested date range is outside both datasets. "
                        f"MERRA-2: {self._merra2_time_range[0].date() if self._merra2_time_range else 'unknown'} to {self._merra2_time_range[1].date() if self._merra2_time_range else 'unknown'}, "
                        f"SYN1deg legacy: {self._syn1_legacy_time_range[0].date() if self._syn1_legacy_time_range else 'unknown'} to {self._syn1_legacy_time_range[1].date() if self._syn1_legacy_time_range else 'unknown'}, "
                        f"SYN1deg current: {self._syn1_time_range[0].date() if self._syn1_time_range else 'unknown'} to {self._syn1_time_range[1].date() if self._syn1_time_range else 'unknown'}"
                    )
                
                raise ValueError(error_msg)
            
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
                f"Successfully fetched NASA POWER data: {len(df)} days for ({latitude}, {longitude}), "
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
        
        if self._syn1_legacy_ds is not None:
            time_values = self._syn1_legacy_ds.time.values
            metadata['solar_legacy'] = {
                'variables': list(self._syn1_legacy_ds.data_vars),
                'time_range': {
                    'start': str(time_values[0]),
                    'end': str(time_values[-1]),
                    'total_days': len(time_values)
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
        if self._syn1_legacy_ds is not None:
            self._syn1_legacy_ds.close()
            self._syn1_legacy_ds = None

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
