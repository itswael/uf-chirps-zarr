"""
Centralized configuration for the CHIRPS Zarr climate data platform.

This module provides a singleton configuration class that manages all
application settings, paths, and parameters. Configuration values can be
overridden via environment variables.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


class Config:
    """
    Centralized configuration for the climate data platform.
    
    All configuration values have sensible defaults but can be overridden
    via environment variables. Directory paths are automatically validated
    and created if they don't exist.
    
    Attributes:
        BASE_DIR: Root directory of the project
        RAW_DIR: Directory for raw downloaded TIFF files
        INTERIM_DIR: Directory for preprocessed intermediate files
        ZARR_DIR: Directory for converted Zarr stores
        DOWNLOAD_CONCURRENCY: Number of concurrent download workers
        CHUNK_SIZE: Download chunk size in bytes
        DATA_SOURCE_URL_PATTERN: URL pattern for CHIRPS data downloads
        CHIRPS_BASE_URL: Base URL for CHIRPS data source
        TIMEOUT_SECONDS: HTTP request timeout in seconds
        MAX_RETRIES: Maximum number of retry attempts for failed requests
        RETRY_DELAY_SECONDS: Delay between retry attempts
        BOOTSTRAP_START_DATE: Start date for initial data ingestion
        BOOTSTRAP_END_DATE: End date for initial data ingestion
        METADATA_CONFIG_PATH: Path to external metadata configuration file
    """
    
    def __init__(self) -> None:
        """Initialize configuration and validate/create required directories."""
        # Base directories
        self._base_dir = Path(
            os.getenv("CHIRPS_BASE_DIR", Path(__file__).parent.parent.resolve())
        )
        
        # Data directories
        self._raw_dir = self._base_dir / os.getenv("CHIRPS_RAW_DIR", "data/raw")
        self._interim_dir = self._base_dir / os.getenv("CHIRPS_INTERIM_DIR", "data/interim")
        self._zarr_dir = self._base_dir / os.getenv("CHIRPS_ZARR_DIR", "data/zarr")
        
        # Download configuration
        self._download_concurrency = int(os.getenv("CHIRPS_DOWNLOAD_CONCURRENCY", "10"))
        self._chunk_size = int(os.getenv("CHIRPS_CHUNK_SIZE", str(8 * 1024 * 1024)))  # 8MB default
        
        # CHIRPS data source configuration
        self._chirps_base_url = os.getenv(
            "CHIRPS_BASE_URL",
            "https://data.chc.ucsb.edu/products/CHIRPS/v3.0"
        )
        self._data_source_url_pattern = os.getenv(
            "CHIRPS_URL_PATTERN",
            "{base_url}/daily/final/rnl/{year}/chirps-v3.0.rnl.{year}.{month:02d}.{day:02d}.tif"
        )
        
        # Network configuration
        self._timeout_seconds = int(os.getenv("CHIRPS_TIMEOUT_SECONDS", "300"))
        self._max_retries = int(os.getenv("CHIRPS_MAX_RETRIES", "3"))
        self._retry_delay_seconds = int(os.getenv("CHIRPS_RETRY_DELAY_SECONDS", "5"))
        
        # Bootstrap configuration
        self._bootstrap_start_date = datetime.strptime(
            os.getenv("CHIRPS_BOOTSTRAP_START_DATE", "2020-01-01"),
            "%Y-%m-%d"
        ).date()
        self._bootstrap_end_date = datetime.strptime(
            os.getenv("CHIRPS_BOOTSTRAP_END_DATE", "2020-12-31"),
            "%Y-%m-%d"
        ).date()
        
        # Incremental ingestion configuration
        # None = no limit (downloads all available data up to today)
        # Set to positive integer to limit days per run
        max_days_env = os.getenv("CHIRPS_INCREMENTAL_MAX_DAYS_PER_RUN")
        self._incremental_max_days_per_run = int(max_days_env) if max_days_env else None
        
        # Metadata configuration
        self._metadata_config_path = self._base_dir / os.getenv(
            "CHIRPS_METADATA_CONFIG",
            "config/metadata.json"
        )
        
        # Zarr storage configuration
        self._zarr_store_name = os.getenv("CHIRPS_ZARR_STORE_NAME", "chirps_v3.0_daily_precip_v1.0.zarr")
        
        # Zarr chunking configuration (fixed and immutable)
        self._zarr_chunk_time = int(os.getenv("CHIRPS_ZARR_CHUNK_TIME", "30"))
        self._zarr_chunk_lat = int(os.getenv("CHIRPS_ZARR_CHUNK_LAT", "250"))
        self._zarr_chunk_lon = int(os.getenv("CHIRPS_ZARR_CHUNK_LON", "250"))
        
        # Zarr compression configuration
        self._zarr_compressor = os.getenv("CHIRPS_ZARR_COMPRESSOR", "blosc")
        self._zarr_compression_level = int(os.getenv("CHIRPS_ZARR_COMPRESSION_LEVEL", "3"))
        
        # Data validation settings
        self._precipitation_fill_value = float(os.getenv("CHIRPS_FILL_VALUE", "-99.0"))
        self._precipitation_valid_min = float(os.getenv("CHIRPS_VALID_MIN", "0.0"))
        
        # Create required directories
        self._ensure_directories()
    
    @property
    def BASE_DIR(self) -> Path:
        """Root directory of the project."""
        return self._base_dir
    
    @property
    def RAW_DIR(self) -> Path:
        """Directory for raw downloaded TIFF files."""
        return self._raw_dir
    
    @property
    def INTERIM_DIR(self) -> Path:
        """Directory for preprocessed intermediate files."""
        return self._interim_dir
    
    @property
    def ZARR_DIR(self) -> Path:
        """Directory for converted Zarr stores."""
        return self._zarr_dir
    
    @property
    def ZARR_STORE_PATH(self) -> Path:
        """Full path to the primary Zarr store."""
        return self._zarr_dir / self._zarr_store_name
    
    @property
    def ZARR_CHUNK_TIME(self) -> int:
        """Zarr chunk size for time dimension."""
        return self._zarr_chunk_time
    
    @property
    def ZARR_CHUNK_LAT(self) -> int:
        """Zarr chunk size for latitude dimension."""
        return self._zarr_chunk_lat
    
    @property
    def ZARR_CHUNK_LON(self) -> int:
        """Zarr chunk size for longitude dimension."""
        return self._zarr_chunk_lon
    
    @property
    def ZARR_COMPRESSOR(self) -> str:
        """Zarr compressor type."""
        return self._zarr_compressor
    
    @property
    def ZARR_COMPRESSION_LEVEL(self) -> int:
        """Zarr compression level."""
        return self._zarr_compression_level
    
    @property
    def PRECIPITATION_FILL_VALUE(self) -> float:
        """Fill value for precipitation data."""
        return self._precipitation_fill_value
    
    @property
    def PRECIPITATION_VALID_MIN(self) -> float:
        """Minimum valid precipitation value."""
        return self._precipitation_valid_min
    
    @property
    def DOWNLOAD_CONCURRENCY(self) -> int:
        """Number of concurrent download workers."""
        return self._download_concurrency
    
    @property
    def CHUNK_SIZE(self) -> int:
        """Download chunk size in bytes."""
        return self._chunk_size
    
    @property
    def CHIRPS_BASE_URL(self) -> str:
        """Base URL for CHIRPS data source."""
        return self._chirps_base_url
    
    @property
    def DATA_SOURCE_URL_PATTERN(self) -> str:
        """URL pattern for constructing CHIRPS download URLs."""
        return self._data_source_url_pattern
    
    @property
    def TIMEOUT_SECONDS(self) -> int:
        """HTTP request timeout in seconds."""
        return self._timeout_seconds
    
    @property
    def MAX_RETRIES(self) -> int:
        """Maximum number of retry attempts for failed requests."""
        return self._max_retries
    
    @property
    def RETRY_DELAY_SECONDS(self) -> int:
        """Delay between retry attempts in seconds."""
        return self._retry_delay_seconds
    
    @property
    def BOOTSTRAP_START_DATE(self) -> datetime.date:
        """Start date for initial data ingestion."""
        return self._bootstrap_start_date
    
    @property
    def BOOTSTRAP_END_DATE(self) -> datetime.date:
        """End date for initial data ingestion."""
        return self._bootstrap_end_date
    
    @property
    def INCREMENTAL_MAX_DAYS_PER_RUN(self) -> Optional[int]:
        """Maximum days per incremental run (None = unlimited, up to today)."""
        return self._incremental_max_days_per_run
    
    @property
    def METADATA_CONFIG_PATH(self) -> Path:
        """Path to external metadata configuration file."""
        return self._metadata_config_path
    
    def _ensure_directories(self) -> None:
        """
        Ensure all required directories exist, creating them if necessary.
        
        Raises:
            OSError: If directory creation fails due to permissions or other issues.
        """
        for directory in [self._raw_dir, self._interim_dir, self._zarr_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Ensure metadata config directory exists
        self._metadata_config_path.parent.mkdir(parents=True, exist_ok=True)
    
    def load_metadata_config(self) -> Dict[str, Any]:
        """
        Load metadata configuration from external JSON file.
        
        The metadata configuration allows clients to customize attributes
        that will be included in the output Zarr store.
        
        Returns:
            Dictionary containing metadata configuration. Returns default
            metadata if config file doesn't exist.
            
        Raises:
            json.JSONDecodeError: If the metadata file contains invalid JSON.
        """
        if not self._metadata_config_path.exists():
            return self._get_default_metadata()
        
        with open(self._metadata_config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _get_default_metadata(self) -> Dict[str, Any]:
        """
        Get default metadata configuration.
        
        Returns:
            Dictionary containing default metadata attributes.
        """
        return {
            "title": "CHIRPS Daily Precipitation Data",
            "institution": "Climate Hazards Center, UC Santa Barbara",
            "source": "CHIRPS version 2.0",
            "references": "Funk et al. 2015, Scientific Data",
            "comment": "Climate Hazards Group InfraRed Precipitation with Station data",
            "Conventions": "CF-1.8",
            "history": f"Created on {datetime.now(timezone.utc).isoformat()}Z"
        }
    
    def save_default_metadata_config(self) -> None:
        """
        Save default metadata configuration to file.
        
        This creates a template metadata configuration file that clients
        can customize.
        
        Raises:
            OSError: If file cannot be written.
        """
        metadata = self._get_default_metadata()
        with open(self._metadata_config_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
    
    def get_chirps_url(self, year: int, month: int, day: int) -> str:
        """
        Construct CHIRPS download URL for a specific date.
        
        Args:
            year: Year (e.g., 2020)
            month: Month (1-12)
            day: Day of month (1-31)
            
        Returns:
            Fully constructed URL for downloading CHIRPS data.
            
        Example:
            >>> config = Config()
            >>> config.get_chirps_url(2025, 1, 15)
            'https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/rnl/2025/chirps-v3.0.rnl.2025.01.15.tif'
        """
        return self._data_source_url_pattern.format(
            base_url=self._chirps_base_url,
            year=year,
            month=month,
            day=day
        )
    
    def get_logger_name(self, module_name: str) -> str:
        """
        Generate standardized logger name for a module.
        
        Args:
            module_name: Name of the module (e.g., __name__)
            
        Returns:
            Standardized logger name.
            
        Example:
            >>> config = Config()
            >>> config.get_logger_name('src.download.chirps_downloader')
            'chirps_zarr.download.chirps_downloader'
        """
        # Remove 'src.' prefix if present for cleaner logger names
        clean_name = module_name.replace('src.', '')
        return f"chirps_zarr.{clean_name}"


# Singleton instance
_config: Optional[Config] = None


def get_config() -> Config:
    """
    Get the singleton configuration instance.
    
    Returns:
        The global Config instance.
    """
    global _config
    if _config is None:
        _config = Config()
    return _config
