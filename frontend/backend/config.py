"""
Backend Configuration
All configurable parameters for the weather data processing system
"""
import os
from pathlib import Path
from typing import Optional


class Config:
    """Central configuration for backend services"""
    
    # ==================== Paths ====================
    BACKEND_DIR = Path(__file__).resolve().parent
    PROJECT_ROOT = BACKEND_DIR.parent.parent
    ZARR_PATH = PROJECT_ROOT / "data" / "zarr" / "chirps_v3.0_daily_precip_v1.0.zarr"
    
    # ==================== Processing Settings ====================
    # Maximum points allowed in a single shapefile upload
    MAX_SHAPEFILE_POINTS: int = int(os.getenv("MAX_SHAPEFILE_POINTS", "1000"))
    
    # Number of coordinates to process in each batch
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "50"))
    
    # Maximum concurrent workers for async processing
    # None = use default (min(32, CPU count + 4))
    MAX_WORKERS: Optional[int] = None
    _max_workers_env = os.getenv("MAX_WORKERS")
    if _max_workers_env is not None and _max_workers_env.strip():
        MAX_WORKERS = int(_max_workers_env)
    
    # Use ProcessPoolExecutor instead of ThreadPoolExecutor
    USE_PROCESSES: bool = os.getenv("USE_PROCESSES", "false").lower() == "true"
    
    # ==================== API Settings ====================
    # Request timeout for multi-point processing (seconds)
    MULTI_POINT_TIMEOUT: int = int(os.getenv("MULTI_POINT_TIMEOUT", "300"))
    
    # CORS allowed origins
    CORS_ORIGINS = [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://10.242.84.63:3000",
        "http://10.242.84.63:3001"
        "http://0.0.0.0:3000",
        "http://0.0.0.0:3001"
    ]
    
    # ==================== ICASA Settings ====================
    # Default site code for ICASA files
    DEFAULT_SITE_CODE: str = os.getenv("DEFAULT_SITE_CODE", "UFLC")
    
    # Variables to include in ICASA output (comma-separated in env var)
    DEFAULT_VARIABLES: list = (
        os.getenv("DEFAULT_VARIABLES", "precipitation").split(",")
    )
    
    # Coordinate precision (decimal places)
    COORDINATE_PRECISION: int = int(os.getenv("COORDINATE_PRECISION", "4"))
    
    # Value precision for weather variables (decimal places)
    VALUE_PRECISION: int = int(os.getenv("VALUE_PRECISION", "1"))
    
    # ==================== NASA POWER Settings ====================
    # Enable NASA POWER data integration
    ENABLE_NASA_POWER: bool = os.getenv("ENABLE_NASA_POWER", "true").lower() == "true"
    
    # Default rain data source: "chirps", "nasa_power", or "both"
    DEFAULT_RAIN_SOURCE: str = os.getenv("DEFAULT_RAIN_SOURCE", "both")
    
    # Available weather variables for plotting
    AVAILABLE_PLOT_VARIABLES: list = [
        "RAIN1",     # CHIRPS Precipitation
        "RAIN2",     # NASA POWER Precipitation
        "TMAX",      # Maximum Temperature
        "TMIN",      # Minimum Temperature
        "T2M",       # Average Temperature
        "SRAD",      # Solar Radiation
        "WIND",      # Wind Speed
        "TDEW",      # Dew Point Temperature
        "RH2M"       # Relative Humidity
    ]
    
    # Default plot variable (CHIRPS rain for backward compatibility)
    DEFAULT_PLOT_VARIABLE: str = os.getenv("DEFAULT_PLOT_VARIABLE", "RAIN1")
    
    # SSL Certificate Verification for NASA POWER S3 access
    # Set to False to disable SSL verification (not recommended for production)
    # Use this only if you're experiencing SSL certificate issues on macOS
    NASA_POWER_VERIFY_SSL: bool = os.getenv("NASA_POWER_VERIFY_SSL", "true").lower() == "true"
    
    # Custom SSL certificate bundle path (optional)
    # Set this if you need to use a custom CA bundle
    NASA_POWER_SSL_CERT_PATH: Optional[str] = os.getenv("NASA_POWER_SSL_CERT_PATH")
    
    # ==================== Validation Settings ====================
    # Valid latitude range
    LAT_BOUNDS: tuple = (-90.0, 90.0)
    
    # Valid longitude range
    LON_BOUNDS: tuple = (-180.0, 180.0)
    
    # ==================== Logging ====================
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # ==================== Performance ====================
    # Zarr chunk sizes for optimal performance
    ZARR_TIME_CHUNKS: int = int(os.getenv("ZARR_TIME_CHUNKS", "100"))
    ZARR_LAT_CHUNKS: int = int(os.getenv("ZARR_LAT_CHUNKS", "100"))
    ZARR_LON_CHUNKS: int = int(os.getenv("ZARR_LON_CHUNKS", "100"))
    
    @classmethod
    def validate(cls):
        """Validate configuration settings"""
        if not cls.ZARR_PATH.exists():
            raise FileNotFoundError(f"Zarr store not found: {cls.ZARR_PATH}")
        
        if cls.MAX_SHAPEFILE_POINTS < 1:
            raise ValueError("MAX_SHAPEFILE_POINTS must be at least 1")
        
        if cls.BATCH_SIZE < 1:
            raise ValueError("BATCH_SIZE must be at least 1")
        
        return True


# Export singleton instance
config = Config()
