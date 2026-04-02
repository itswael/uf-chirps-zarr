"""
Elevation Data Utility
Provides elevation data for specific coordinates using WELEV dataset
Optimized with caching and fast interpolation for parallel processing
"""
import logging
from pathlib import Path
from typing import Optional, Dict, Tuple, List
import xarray as xr
import numpy as np
from scipy.interpolate import RegularGridInterpolator

from .nasa_power_config import nasa_power_config

logger = logging.getLogger(__name__)


class ElevationProvider:
    """
    Provide elevation data from WELEV dataset with caching and fast interpolation.
    Optimized for parallel processing with multiple concurrent requests.
    """
    
    def __init__(self, elevation_file_path: Optional[Path] = None):
        """
        Initialize elevation provider.
        
        Args:
            elevation_file_path: Path to welev_merra2_grid.nc file
        """
        self.elevation_file_path = elevation_file_path or nasa_power_config.ELEVATION_FILE_PATH
        self._dataset: Optional[xr.Dataset] = None
        self._loaded = False
        
        # Fast interpolation setup
        self._interpolator: Optional[RegularGridInterpolator] = None
        self._lats: Optional[np.ndarray] = None
        self._lons: Optional[np.ndarray] = None
        self._elevation_data: Optional[np.ndarray] = None
        self._npz_cache_path = self.elevation_file_path.with_suffix('.npz')
        
        # Cache for previously fetched elevations
        # Key: (rounded_lat, rounded_lon), Value: elevation
        # Rounding to 4 decimal places (~11m precision) reduces cache size
        self._cache: Dict[Tuple[float, float], float] = {}
        self._cache_hits = 0
        self._cache_misses = 0
    
    def load(self):
        """Load the elevation dataset and prepare fast interpolator"""
        if self._loaded:
            return
        
        try:
            if not self.elevation_file_path.exists():
                logger.warning(
                    f"Elevation file not found: {self.elevation_file_path}. "
                    f"Will use default elevation value."
                )
                return

            loaded_from_npz = False
            if self._npz_cache_path.exists() and self._npz_cache_path.stat().st_mtime >= self.elevation_file_path.stat().st_mtime:
                try:
                    cached = np.load(self._npz_cache_path)
                    self._lats = cached['lats']
                    self._lons = cached['lons']
                    self._elevation_data = cached['elev']
                    loaded_from_npz = True
                    logger.info(f"Loaded elevation arrays from cache: {self._npz_cache_path}")
                except Exception as cache_err:
                    logger.warning(f"Failed to load elevation cache {self._npz_cache_path}: {cache_err}")

            if not loaded_from_npz:
                # Load dataset and extract numpy arrays.
                self._dataset = xr.open_dataset(self.elevation_file_path)

                if 'WELEV' in self._dataset:
                    welev_data = self._dataset['WELEV']
                    self._lats = welev_data.coords['y'].values
                    self._lons = welev_data.coords['x'].values
                    self._elevation_data = welev_data.values

                    # Persist sidecar cache for faster cold start on next run.
                    try:
                        np.savez_compressed(
                            self._npz_cache_path,
                            lats=self._lats,
                            lons=self._lons,
                            elev=self._elevation_data,
                        )
                        logger.info(f"Wrote elevation cache: {self._npz_cache_path}")
                    except Exception as cache_write_err:
                        logger.warning(f"Could not write elevation cache: {cache_write_err}")

            if self._elevation_data is not None and self._lats is not None and self._lons is not None:
                # Sanitize invalid DEM values. Values below -430 m are outside realistic land elevations.
                invalid_mask = (~np.isfinite(self._elevation_data)) | (self._elevation_data < -430.0) | (self._elevation_data > 9000.0)
                invalid_count = int(np.count_nonzero(invalid_mask))
                if invalid_count:
                    self._elevation_data = self._elevation_data.astype(np.float32, copy=True)
                    self._elevation_data[invalid_mask] = np.nan
                    logger.warning(f"Sanitized {invalid_count} invalid elevation cells in WELEV grid")

                # Create scipy interpolator (much faster than xarray.interp)
                self._interpolator = RegularGridInterpolator(
                    (self._lats, self._lons),
                    self._elevation_data,
                    method='linear',
                    bounds_error=False,
                    fill_value=nasa_power_config.DEFAULT_ELEVATION
                )

                logger.info(
                    f"Elevation dataset loaded with fast interpolator: "
                    f"{self.elevation_file_path} "
                    f"(shape: {self._elevation_data.shape})"
                )
            
            self._loaded = True
            
        except Exception as e:
            logger.error(f"Error loading elevation dataset: {e}")
            # Don't raise - we'll use default elevation
    
    def _round_coords(self, lat: float, lon: float) -> Tuple[float, float]:
        """Round coordinates for cache key (4 decimal places = ~11m precision)"""
        return (round(lat, 4), round(lon, 4))
    
    def get_elevation(self, lat: float, lon: float) -> float:
        """
        Get elevation for a specific latitude and longitude with caching.
        
        Args:
            lat: Latitude of the location
            lon: Longitude of the location
            
        Returns:
            Elevation in meters
        """
        # Check cache first
        cache_key = self._round_coords(lat, lon)
        if cache_key in self._cache:
            self._cache_hits += 1
            return self._cache[cache_key]
        
        self._cache_misses += 1
        
        # Load dataset if not already loaded
        if not self._loaded:
            self.load()
        
        # If no interpolator available, use default
        if self._interpolator is None:
            logger.debug(
                f"Elevation data not available, using default: "
                f"{nasa_power_config.DEFAULT_ELEVATION}"
            )
            elevation = nasa_power_config.DEFAULT_ELEVATION
        else:
            try:
                # Use fast scipy interpolator
                elevation = float(self._interpolator((lat, lon)))
                if not np.isfinite(elevation):
                    elevation = nasa_power_config.DEFAULT_ELEVATION
                elevation = round(elevation, 2)
                
            except Exception as e:
                logger.warning(f"Error getting elevation for ({lat}, {lon}): {e}")
                elevation = nasa_power_config.DEFAULT_ELEVATION
        
        # Cache the result
        self._cache[cache_key] = elevation
        
        # Log cache stats periodically (every 100 misses)
        if self._cache_misses % 100 == 0:
            total = self._cache_hits + self._cache_misses
            hit_rate = (self._cache_hits / total * 100) if total > 0 else 0
            logger.info(
                f"Elevation cache stats: {self._cache_hits} hits, "
                f"{self._cache_misses} misses ({hit_rate:.1f}% hit rate), "
                f"cache size: {len(self._cache)}"
            )
        
        return elevation
    
    def get_elevations_batch(self, coordinates: List[Tuple[float, float]]) -> List[float]:
        """
        Get elevations for multiple coordinates in batch (faster than individual calls).
        
        Args:
            coordinates: List of (lat, lon) tuples
            
        Returns:
            List of elevations in meters
        """
        if not self._loaded:
            self.load()
        
        # If no interpolator, return defaults
        if self._interpolator is None:
            return [nasa_power_config.DEFAULT_ELEVATION] * len(coordinates)
        
        elevations = []
        uncached_indices = []
        uncached_coords = []
        
        # Check cache for all coordinates
        for i, (lat, lon) in enumerate(coordinates):
            cache_key = self._round_coords(lat, lon)
            if cache_key in self._cache:
                elevations.append(self._cache[cache_key])
                self._cache_hits += 1
            else:
                elevations.append(None)  # Placeholder
                uncached_indices.append(i)
                uncached_coords.append((lat, lon))
                self._cache_misses += 1
        
        # Batch fetch uncached elevations
        if uncached_coords:
            try:
                # Vectorized interpolation (much faster)
                uncached_elevations = self._interpolator(uncached_coords)
                uncached_elevations = np.round(uncached_elevations, 2)
                
                # Fill in results and update cache
                for idx, elev, (lat, lon) in zip(uncached_indices, uncached_elevations, uncached_coords):
                    elev_float = float(elev) if np.isfinite(elev) else nasa_power_config.DEFAULT_ELEVATION
                    elevations[idx] = elev_float
                    cache_key = self._round_coords(lat, lon)
                    self._cache[cache_key] = elev_float
                    
            except Exception as e:
                logger.warning(f"Error in batch elevation fetch: {e}")
                # Fill with defaults
                for idx in uncached_indices:
                    elevations[idx] = nasa_power_config.DEFAULT_ELEVATION
        
        return elevations
    
    def clear_cache(self):
        """Clear the elevation cache"""
        self._cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0
        logger.info("Elevation cache cleared")
    
    def close(self):
        """Close the elevation dataset"""
        if self._dataset is not None:
            self._dataset.close()
            self._dataset = None
        self._loaded = False
        self._interpolator = None
        self._elevation_data = None
        self._lats = None
        self._lons = None


# Global elevation provider instance
_global_elevation_provider: Optional[ElevationProvider] = None


def get_elevation_provider() -> ElevationProvider:
    """
    Get or create the global elevation provider instance.
    
    Returns:
        ElevationProvider instance
    """
    global _global_elevation_provider
    if _global_elevation_provider is None:
        _global_elevation_provider = ElevationProvider()
    return _global_elevation_provider


def get_elevation(lat: float, lon: float) -> float:
    """
    Convenience function to get elevation for a coordinate.
    
    Args:
        lat: Latitude
        lon: Longitude
        
    Returns:
        Elevation in meters
    """
    provider = get_elevation_provider()
    return provider.get_elevation(lat, lon)
