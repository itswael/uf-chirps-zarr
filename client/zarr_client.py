"""
CHIRPS Zarr Client for various access patterns.

This module provides a client interface for accessing the CHIRPS Zarr store
with support for different access patterns, spatial/temporal subsetting,
and concurrent operations.
"""
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import xarray as xr
from config import config


class ChirpsZarrClient:
    """Client for accessing CHIRPS Zarr data with various patterns."""
    
    def __init__(self, zarr_path: Optional[Path] = None):
        """
        Initialize CHIRPS Zarr client.
        
        Args:
            zarr_path: Path to Zarr store (defaults to config path)
        """
        self.zarr_path = zarr_path or config.zarr_path
        if not self.zarr_path.exists():
            raise FileNotFoundError(f"Zarr store not found: {self.zarr_path}")
        
        self._dataset = None
        
    def open(self) -> xr.Dataset:
        """Open the Zarr dataset."""
        if self._dataset is None:
            self._dataset = xr.open_zarr(self.zarr_path, chunks=config.dask_chunks)
        return self._dataset
    
    def close(self):
        """Close the dataset."""
        if self._dataset is not None:
            self._dataset.close()
            self._dataset = None
    
    def __enter__(self):
        """Context manager entry."""
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    # ========================================================================
    # Basic Access Patterns
    # ========================================================================
    
    def get_single_date(self, date: str) -> xr.Dataset:
        """
        Get data for a single date.
        
        Args:
            date: Date string in YYYY-MM-DD format
            
        Returns:
            Dataset for the specified date
        """
        ds = self.open()
        return ds.sel(time=date)
    
    def get_date_range(self, start_date: str, end_date: str) -> xr.Dataset:
        """
        Get data for a date range.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Dataset for the date range
        """
        ds = self.open()
        return ds.sel(time=slice(start_date, end_date))
    
    def get_spatial_subset(
        self,
        lon_min: float,
        lon_max: float,
        lat_min: float,
        lat_max: float,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> xr.Dataset:
        """
        Get spatial subset of data.
        
        Args:
            lon_min: Minimum longitude
            lon_max: Maximum longitude
            lat_min: Minimum latitude
            lat_max: Maximum latitude
            start_date: Optional start date
            end_date: Optional end date
            
        Returns:
            Spatially subsetted dataset
        """
        ds = self.open()
        subset = ds.sel(
            longitude=slice(lon_min, lon_max),
            latitude=slice(lat_max, lat_min)  # Latitude is descending
        )
        
        if start_date and end_date:
            subset = subset.sel(time=slice(start_date, end_date))
        
        return subset
    
    def get_region(
        self,
        region_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> xr.Dataset:
        """
        Get data for a named region.
        
        Args:
            region_name: Name of region from config
            start_date: Optional start date
            end_date: Optional end date
            
        Returns:
            Regional subset
        """
        lon_min, lon_max, lat_min, lat_max = config.get_region_bounds(region_name)
        return self.get_spatial_subset(lon_min, lon_max, lat_min, lat_max, start_date, end_date)
    
    # ========================================================================
    # Aggregation Operations
    # ========================================================================
    
    def compute_temporal_mean(
        self,
        start_date: str,
        end_date: str,
        region: Optional[str] = None,
    ) -> xr.DataArray:
        """
        Compute temporal mean over date range.
        
        Args:
            start_date: Start date
            end_date: End date
            region: Optional region name
            
        Returns:
            Temporal mean precipitation
        """
        if region:
            data = self.get_region(region, start_date, end_date)
        else:
            data = self.get_date_range(start_date, end_date)
        
        return data['precipitation'].mean(dim='time')
    
    def compute_temporal_sum(
        self,
        start_date: str,
        end_date: str,
        region: Optional[str] = None,
    ) -> xr.DataArray:
        """
        Compute temporal sum over date range.
        
        Args:
            start_date: Start date
            end_date: End date
            region: Optional region name
            
        Returns:
            Accumulated precipitation
        """
        if region:
            data = self.get_region(region, start_date, end_date)
        else:
            data = self.get_date_range(start_date, end_date)
        
        return data['precipitation'].sum(dim='time')
    
    def compute_spatial_mean(self, date: str) -> float:
        """
        Compute spatial mean for a given date.
        
        Args:
            date: Date string
            
        Returns:
            Global mean precipitation
        """
        data = self.get_single_date(date)
        return float(data['precipitation'].mean().compute())
    
    # ========================================================================
    # Concurrent Access Patterns
    # ========================================================================
    
    def parallel_date_access(
        self,
        dates: List[str],
        max_workers: Optional[int] = None,
    ) -> Dict[str, xr.Dataset]:
        """
        Access multiple dates in parallel using threads.
        
        Args:
            dates: List of date strings
            max_workers: Number of worker threads
            
        Returns:
            Dictionary mapping dates to datasets
        """
        max_workers = max_workers or config.max_workers
        results = {}
        
        def fetch_date(date: str) -> Tuple[str, xr.Dataset]:
            # Each thread needs its own dataset instance
            with ChirpsZarrClient(self.zarr_path) as client:
                return date, client.get_single_date(date)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_date = {executor.submit(fetch_date, date): date for date in dates}
            
            for future in as_completed(future_to_date):
                date, data = future.result()
                results[date] = data
        
        return results
    
    async def async_date_access(self, dates: List[str]) -> Dict[str, xr.Dataset]:
        """
        Access multiple dates asynchronously.
        
        Args:
            dates: List of date strings
            
        Returns:
            Dictionary mapping dates to datasets
        """
        async def fetch_date(date: str) -> Tuple[str, xr.Dataset]:
            # Run in thread pool since xarray I/O is blocking
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                lambda: (date, self.get_single_date(date))
            )
        
        tasks = [fetch_date(date) for date in dates]
        results = await asyncio.gather(*tasks)
        return dict(results)
    
    def parallel_region_analysis(
        self,
        regions: List[str],
        start_date: str,
        end_date: str,
        max_workers: Optional[int] = None,
    ) -> Dict[str, Dict[str, float]]:
        """
        Analyze multiple regions in parallel.
        
        Args:
            regions: List of region names
            start_date: Start date
            end_date: End date
            max_workers: Number of workers
            
        Returns:
            Dictionary with statistics for each region
        """
        max_workers = max_workers or config.max_workers
        results = {}
        
        def analyze_region(region: str) -> Tuple[str, Dict[str, float]]:
            with ChirpsZarrClient(self.zarr_path) as client:
                data = client.get_region(region, start_date, end_date)
                precip = data['precipitation']
                
                stats = {
                    'mean': float(precip.mean().compute()),
                    'std': float(precip.std().compute()),
                    'min': float(precip.min().compute()),
                    'max': float(precip.max().compute()),
                    'sum': float(precip.sum().compute()),
                }
                return region, stats
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(analyze_region, region) for region in regions]
            
            for future in as_completed(futures):
                region, stats = future.result()
                results[region] = stats
        
        return results
    
    # ========================================================================
    # Performance Testing
    # ========================================================================
    
    def benchmark_access(
        self,
        access_type: str,
        n_iterations: int = 10,
        **kwargs
    ) -> Dict[str, float]:
        """
        Benchmark different access patterns.
        
        Args:
            access_type: Type of access ('single_date', 'date_range', 'region', etc.)
            n_iterations: Number of iterations
            **kwargs: Arguments for the access method
            
        Returns:
            Timing statistics
        """
        times = []
        
        for _ in range(n_iterations):
            start = time.time()
            
            if access_type == 'single_date':
                _ = self.get_single_date(kwargs['date']).compute()
            elif access_type == 'date_range':
                _ = self.get_date_range(kwargs['start_date'], kwargs['end_date']).compute()
            elif access_type == 'region':
                _ = self.get_region(kwargs['region'], kwargs.get('start_date'), kwargs.get('end_date')).compute()
            elif access_type == 'temporal_mean':
                _ = self.compute_temporal_mean(kwargs['start_date'], kwargs['end_date'], kwargs.get('region')).compute()
            else:
                raise ValueError(f"Unknown access type: {access_type}")
            
            elapsed = time.time() - start
            times.append(elapsed)
        
        return {
            'mean': np.mean(times),
            'std': np.std(times),
            'min': np.min(times),
            'max': np.max(times),
            'total': np.sum(times),
        }
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def get_metadata(self) -> Dict:
        """Get dataset metadata."""
        ds = self.open()
        return {
            'dimensions': dict(ds.dims),
            'coordinates': list(ds.coords),
            'variables': list(ds.data_vars),
            'attributes': dict(ds.attrs),
            'time_range': (str(ds.time.values[0]), str(ds.time.values[-1])),
            'spatial_extent': {
                'lon_min': float(ds.longitude.min()),
                'lon_max': float(ds.longitude.max()),
                'lat_min': float(ds.latitude.min()),
                'lat_max': float(ds.latitude.max()),
            },
            'chunks': {var: ds[var].chunks for var in ds.data_vars},
        }
    
    def validate_data(self, date: str) -> Dict[str, Union[bool, str]]:
        """
        Validate data for a specific date.
        
        Args:
            date: Date to validate
            
        Returns:
            Validation results
        """
        try:
            data = self.get_single_date(date)
            precip = data['precipitation'].values
            
            # Filter out fill values
            valid_precip = precip[precip != -9999.0]
            
            checks = {
                'has_data': len(valid_precip) > 0,
                'no_negatives': not np.any(valid_precip < 0),
                'reasonable_max': float(np.max(valid_precip)) < 2000,  # mm/day
                'mean_value': float(np.mean(valid_precip)),
                'valid_percent': 100 * len(valid_precip) / precip.size,
            }
            
            checks['all_passed'] = (
                checks['has_data'] and 
                checks['no_negatives'] and 
                checks['reasonable_max']
            )
            
            return checks
            
        except Exception as e:
            return {'error': str(e), 'all_passed': False}
