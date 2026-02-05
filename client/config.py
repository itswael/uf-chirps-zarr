"""
Client configuration for CHIRPS Zarr access.

This module contains configuration specific to the client applications
for accessing and testing the CHIRPS Zarr store.
"""
import os
from pathlib import Path
from typing import Dict, Tuple


class ClientConfig:
    """Configuration for CHIRPS Zarr client."""
    
    def __init__(self):
        # Base paths
        self.base_dir = Path(__file__).parent.parent
        self.zarr_path = self.base_dir / "data" / "zarr" / "chirps_v3.0_daily_precip_v1.0.zarr"
        
        # Access patterns configuration
        self.chunk_cache_size = int(os.getenv("ZARR_CHUNK_CACHE_MB", "256")) * 1024 * 1024
        self.max_workers = int(os.getenv("CLIENT_MAX_WORKERS", "4"))
        
        # Test regions (lon_min, lon_max, lat_min, lat_max)
        self.test_regions: Dict[str, Tuple[float, float, float, float]] = {
            "angola": (14.0, 21.0, -17.0, -8.0),
            "west_africa": (-10.0, 17.0, 7.0, 28.0),
            "southern_africa": (18.0, 29.0, -29.0, -10.0),
            "sahel": (-2.5, 1, 12, 15.5),
            "horn_of_africa": (39.0, 46.0, 3.0, 10.0),
            "wyoming": (-111.0, -104.0, 41.0, 45.0),
        }
        
        # Test date ranges
        self.test_date_ranges = {
            "single_day": ("2024-01-01", "2024-01-01"),
            "one_week": ("2024-01-01", "2024-01-07"),
            "one_month": ("2024-01-01", "2024-01-31"),
            "three_months": ("2024-01-01", "2024-03-31"),
            "one_year": ("2023-01-01", "2023-12-31"),
            "full_dataset": ("2023-01-01", "2024-12-31"),
        }
        
        # Performance tuning
        self.dask_chunks = {
            "time": 30,
            "latitude": 500,
            "longitude": 500,
        }
        
    def get_region_bounds(self, region_name: str) -> Tuple[float, float, float, float]:
        """Get bounding box for a named region."""
        if region_name not in self.test_regions:
            raise ValueError(f"Unknown region: {region_name}. Available: {list(self.test_regions.keys())}")
        return self.test_regions[region_name]
    
    def get_date_range(self, range_name: str) -> Tuple[str, str]:
        """Get date range by name."""
        if range_name not in self.test_date_ranges:
            raise ValueError(f"Unknown date range: {range_name}. Available: {list(self.test_date_ranges.keys())}")
        return self.test_date_ranges[range_name]


# Global config instance
config = ClientConfig()
