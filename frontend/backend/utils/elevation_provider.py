"""
Elevation Data Utility
Provides elevation data for specific coordinates using WELEV dataset
"""
import logging
from pathlib import Path
import xarray as xr
from typing import Optional

from .nasa_power_config import nasa_power_config

logger = logging.getLogger(__name__)


class ElevationProvider:
    """Provide elevation data from WELEV dataset"""
    
    def __init__(self, elevation_file_path: Optional[Path] = None):
        """
        Initialize elevation provider.
        
        Args:
            elevation_file_path: Path to welev_merra2_grid.nc file
        """
        self.elevation_file_path = elevation_file_path or nasa_power_config.ELEVATION_FILE_PATH
        self._dataset: Optional[xr.Dataset] = None
        self._loaded = False
    
    def load(self):
        """Load the elevation dataset"""
        if self._loaded:
            return
        
        try:
            if not self.elevation_file_path.exists():
                logger.warning(
                    f"Elevation file not found: {self.elevation_file_path}. "
                    f"Will use default elevation value."
                )
                return
            
            self._dataset = xr.open_dataset(self.elevation_file_path)
            self._loaded = True
            logger.info(f"Elevation dataset loaded: {self.elevation_file_path}")
            
        except Exception as e:
            logger.error(f"Error loading elevation dataset: {e}")
            # Don't raise - we'll use default elevation
    
    def get_elevation(self, lat: float, lon: float) -> float:
        """
        Get elevation for a specific latitude and longitude.
        
        Args:
            lat: Latitude of the location
            lon: Longitude of the location
            
        Returns:
            Elevation in meters
        """
        if not self._loaded:
            self.load()
        
        if self._dataset is None or 'WELEV' not in self._dataset:
            logger.debug(
                f"Elevation data not available, using default: "
                f"{nasa_power_config.DEFAULT_ELEVATION}"
            )
            return nasa_power_config.DEFAULT_ELEVATION
        
        try:
            welev_data = self._dataset['WELEV']
            
            # Interpolate to exact coordinates using linear interpolation
            # This matches the pythia_weather implementation
            elevation = welev_data.interp(y=lat, x=lon, method='linear')
            
            elev_value = float(elevation.values.item())
            
            # Return rounded elevation
            return round(elev_value, 2)
            
        except Exception as e:
            logger.warning(f"Error getting elevation for ({lat}, {lon}): {e}")
            return nasa_power_config.DEFAULT_ELEVATION
    
    def close(self):
        """Close the elevation dataset"""
        if self._dataset is not None:
            self._dataset.close()
            self._dataset = None
        self._loaded = False


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
