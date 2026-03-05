"""
NASA POWER Configuration
Constants and mappings for NASA POWER S3 data integration
"""
from typing import Dict, List
from pathlib import Path


class NasaPowerConfig:
    """NASA POWER S3 configuration"""
    
    # ==================== S3 URLs ====================
    NASA_POWER_S3_BASE = "https://nasa-power.s3.us-west-2.amazonaws.com/"
    
    # Known daily Zarr roots for POWER ARD on AWS S3 (public/anonymous)
    SYN1DAILY_ZARR_URL = (
        "https://nasa-power.s3.us-west-2.amazonaws.com/"
        "syn1deg/temporal/power_syn1deg_daily_temporal_lst.zarr"
    )
    MERRA2DAILY_ZARR_URL = (
        "https://nasa-power.s3.us-west-2.amazonaws.com/"
        "merra2/temporal/power_merra2_daily_temporal_lst.zarr"
    )
    
    # ==================== Variable Sets ====================
    # Solar variables (from SYN1deg)
    SOLAR_VARS = ["ALLSKY_SFC_SW_DWN"]  # SRAD source (W m^-2) -> convert to MJ m^-2 d^-1
    
    # Meteorological variables (from MERRA-2)
    MET_VARS = [
        "T2M",              # Average temperature at 2m (°C)
        "T2M_MAX",          # Maximum temperature at 2m (°C)
        "T2M_MIN",          # Minimum temperature at 2m (°C)
        "PRECTOTCORR",      # Precipitation corrected (mm/day)
        "T2MDEW",           # Dew point temperature at 2m (°C)
        "WS2M",             # Wind speed at 2m (m/s)
        "RH2M"              # Relative humidity at 2m (%)
    ]
    
    # ==================== Variable Renaming Mappings ====================
    # NASA POWER names to ICASA standard names
    RENAME_MET_VARS = {
        "T2M_MAX": "TMAX",
        "T2M_MIN": "TMIN",
        "PRECTOTCORR": "RAIN2"  # NASA POWER rain (differentiate from CHIRPS)
    }
    
    RENAME_SOLAR_VARS = {
        "ALLSKY_SFC_SW_DWN": "SRAD_WM2"
    }
    
    # ==================== ICASA Variable Configurations ====================
    # Comprehensive variable configurations for ICASA output
    ICASA_VARIABLE_CONFIGS = {
        'RAIN1': {
            'description': 'Precipitation from CHIRPS (mm/day)',
            'units': 'mm/day',
            'decimal_places': 1,
            'default_value': 0.0,
            'source': 'CHIRPS'
        },
        'RAIN2': {
            'description': 'Precipitation from NASA POWER (mm/day)',
            'units': 'mm/day',
            'decimal_places': 1,
            'default_value': 0.0,
            'source': 'NASA_POWER'
        },
        'RAIN': {
            'description': 'Precipitation (mm/day)',
            'units': 'mm/day',
            'decimal_places': 1,
            'default_value': 0.0,
            'source': 'HYBRID'  # Can be from either source based on user selection
        },
        'T2M': {
            'description': 'Temperature at 2 Meters (°C)',
            'units': '°C',
            'decimal_places': 1,
            'default_value': -99.0,
            'source': 'NASA_POWER'
        },
        'TMAX': {
            'description': 'Temperature at 2 Meters Maximum (°C)',
            'units': '°C',
            'decimal_places': 1,
            'default_value': -99.0,
            'source': 'NASA_POWER'
        },
        'TMIN': {
            'description': 'Temperature at 2 Meters Minimum (°C)',
            'units': '°C',
            'decimal_places': 1,
            'default_value': -99.0,
            'source': 'NASA_POWER'
        },
        'TDEW': {
            'description': 'Dew/Frost Point at 2 Meters (°C)',
            'units': '°C',
            'decimal_places': 1,
            'default_value': -99.0,
            'source': 'NASA_POWER'
        },
        'RH2M': {
            'description': 'Relative Humidity at 2 Meters (%)',
            'units': '%',
            'decimal_places': 1,
            'default_value': -99.0,
            'source': 'NASA_POWER'
        },
        'WIND': {
            'description': 'Wind Speed at 2 Meters (m/s)',
            'units': 'm/s',
            'decimal_places': 1,
            'default_value': -99.0,
            'source': 'NASA_POWER'
        },
        'SRAD': {
            'description': 'All Sky Surface Shortwave Downward Irradiance (MJ/m²/day)',
            'units': 'MJ/m²/day',
            'decimal_places': 1,
            'default_value': -99.0,
            'source': 'NASA_POWER'
        }
    }
    
    # ==================== Variable Mapping ====================
    # Mapping from NASA POWER variables to ICASA codes
    VARIABLE_TO_ICASA_MAP = {
        'T2M': 'T2M',
        'TMAX': 'TMAX',
        'TMIN': 'TMIN',
        'RAIN': 'RAIN',
        'RAIN1': 'RAIN1',
        'RAIN2': 'RAIN2',
        'SRAD': 'SRAD',
        'T2MDEW': 'TDEW',
        'WS2M': 'WIND',
        'RH2M': 'RH2M',
    }
    
    # ==================== Resolution Settings ====================
    # NASA POWER resolution (degrees)
    NASA_POWER_RESOLUTION = 0.5
    
    # CHIRPS resolution (degrees)
    CHIRPS_RESOLUTION = 0.05
    
    # ==================== Default ICASA Header Settings ====================
    DEFAULT_SITE_CODE = "UFLC"
    DEFAULT_ELEVATION = -99.0  # Will be replaced with actual elevation
    REFHT = 2.0  # Reference height for temperature (m)
    WNDHT = 2.0  # Reference height for wind speed (m)
    TAV = -99.0  # Average temperature (°C)
    AMP = -99.0  # Temperature amplitude (°C)
    
    # ==================== ICASA Header Template ====================
    ICASA_HEADER_TEMPLATE = """$WEATHER DATA : {source}

! T2M     Temperature at 2 Meters (°C)
! TMIN     Temperature at 2 Meters Minimum (°C)
! TMAX     Temperature at 2 Meters Maximum (°C)
! TDEW     Dew/Frost Point at 2 Meters (°C)
! RH2M     Relative Humidity at 2 Meters (%)
! RAIN     Precipitation Corrected (mm/day)
! RAIN1    Precipitation from CHIRPS (mm/day)
! RAIN2    Precipitation from NASA POWER (mm/day)
! WIND     Wind Speed at 2 Meters (m/s)
! SRAD     All Sky Surface Shortwave Downward Irradiance (MJ/m²/day)

@ INSI   WTHLAT  WTHLONG   WELEV   TAV   AMP  REFHT  WNDHT"""
    
    # ==================== Elevation Data ====================
    # Path to elevation data file (relative to backend directory)
    ELEVATION_FILE_PATH = Path(__file__).resolve().parent.parent.parent.parent / "pythia_weather" / "welev_merra2_grid.nc"
    
    @classmethod
    def get_available_variables(cls) -> List[str]:
        """Get list of all available ICASA variables"""
        return list(cls.ICASA_VARIABLE_CONFIGS.keys())
    
    @classmethod
    def get_nasa_power_variables(cls) -> List[str]:
        """Get list of variables that come from NASA POWER"""
        return [
            var for var, config in cls.ICASA_VARIABLE_CONFIGS.items()
            if config['source'] == 'NASA_POWER'
        ]
    
    @classmethod
    def get_chirps_variables(cls) -> List[str]:
        """Get list of variables that come from CHIRPS"""
        return [
            var for var, config in cls.ICASA_VARIABLE_CONFIGS.items()
            if config['source'] == 'CHIRPS'
        ]
    
    @classmethod
    def get_variable_config(cls, variable: str) -> Dict:
        """Get configuration for a specific variable"""
        return cls.ICASA_VARIABLE_CONFIGS.get(variable, {})


# Export singleton configuration
nasa_power_config = NasaPowerConfig()
