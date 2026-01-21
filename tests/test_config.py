"""
Tests for the configuration module.

Tests cover directory creation, default values, environment variable
overrides, and utility functions.
"""

import json
import os
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from src.config import Config, get_config


class TestConfigDefaults:
    """Test default configuration values."""
    
    def test_default_download_concurrency(self):
        """Test that download concurrency has correct default value."""
        config = Config()
        assert config.DOWNLOAD_CONCURRENCY == 4
    
    def test_default_chunk_size(self):
        """Test that chunk size has correct default value (8MB)."""
        config = Config()
        assert config.CHUNK_SIZE == 8 * 1024 * 1024
    
    def test_default_timeout(self):
        """Test that timeout has correct default value."""
        config = Config()
        assert config.TIMEOUT_SECONDS == 300
    
    def test_default_max_retries(self):
        """Test that max retries has correct default value."""
        config = Config()
        assert config.MAX_RETRIES == 3
    
    def test_default_retry_delay(self):
        """Test that retry delay has correct default value."""
        config = Config()
        assert config.RETRY_DELAY_SECONDS == 5
    
    def test_default_bootstrap_dates(self):
        """Test that bootstrap dates have correct default values."""
        config = Config()
        assert config.BOOTSTRAP_START_DATE == date(2020, 1, 1)
        assert config.BOOTSTRAP_END_DATE == date(2020, 12, 31)
    
    def test_default_chirps_base_url(self):
        """Test that CHIRPS base URL has correct default value."""
        config = Config()
        assert config.CHIRPS_BASE_URL == "https://data.chc.ucsb.edu/products/CHIRPS/v3.0"
    
    def test_default_data_source_url_pattern(self):
        """Test that URL pattern has correct default value."""
        config = Config()
        expected = "{base_url}/daily/final/rnl/{year}/chirps-v3.0.rnl.{year}.{month:02d}.{day:02d}.tif"
        assert config.DATA_SOURCE_URL_PATTERN == expected


class TestConfigPaths:
    """Test configuration path properties."""
    
    def test_base_dir_is_path(self):
        """Test that BASE_DIR returns a Path object."""
        config = Config()
        assert isinstance(config.BASE_DIR, Path)
    
    def test_raw_dir_is_path(self):
        """Test that RAW_DIR returns a Path object."""
        config = Config()
        assert isinstance(config.RAW_DIR, Path)
    
    def test_interim_dir_is_path(self):
        """Test that INTERIM_DIR returns a Path object."""
        config = Config()
        assert isinstance(config.INTERIM_DIR, Path)
    
    def test_zarr_dir_is_path(self):
        """Test that ZARR_DIR returns a Path object."""
        config = Config()
        assert isinstance(config.ZARR_DIR, Path)
    
    def test_zarr_store_path(self):
        """Test that ZARR_STORE_PATH is constructed correctly."""
        config = Config()
        assert config.ZARR_STORE_PATH == config.ZARR_DIR / "chirps_v3.0_daily_precip_v1.0.zarr"
    
    def test_metadata_config_path_is_path(self):
        """Test that METADATA_CONFIG_PATH returns a Path object."""
        config = Config()
        assert isinstance(config.METADATA_CONFIG_PATH, Path)


class TestDirectoryCreation:
    """Test that required directories are created."""
    
    def test_raw_directory_created(self):
        """Test that RAW_DIR is created on initialization."""
        config = Config()
        assert config.RAW_DIR.exists()
        assert config.RAW_DIR.is_dir()
    
    def test_interim_directory_created(self):
        """Test that INTERIM_DIR is created on initialization."""
        config = Config()
        assert config.INTERIM_DIR.exists()
        assert config.INTERIM_DIR.is_dir()
    
    def test_zarr_directory_created(self):
        """Test that ZARR_DIR is created on initialization."""
        config = Config()
        assert config.ZARR_DIR.exists()
        assert config.ZARR_DIR.is_dir()
    
    def test_metadata_config_directory_created(self):
        """Test that metadata config directory is created."""
        config = Config()
        assert config.METADATA_CONFIG_PATH.parent.exists()
        assert config.METADATA_CONFIG_PATH.parent.is_dir()


class TestEnvironmentVariableOverrides:
    """Test that environment variables override default values."""
    
    def test_base_dir_override(self, tmp_path):
        """Test that BASE_DIR can be overridden via environment variable."""
        test_dir = tmp_path / "test_base"
        with patch.dict(os.environ, {"CHIRPS_BASE_DIR": str(test_dir)}):
            config = Config()
            assert config.BASE_DIR == test_dir
    
    def test_raw_dir_override(self):
        """Test that RAW_DIR can be overridden via environment variable."""
        with patch.dict(os.environ, {"CHIRPS_RAW_DIR": "custom/raw"}):
            config = Config()
            assert "custom" in str(config.RAW_DIR)
            assert "raw" in str(config.RAW_DIR)
    
    def test_download_concurrency_override(self):
        """Test that DOWNLOAD_CONCURRENCY can be overridden."""
        with patch.dict(os.environ, {"CHIRPS_DOWNLOAD_CONCURRENCY": "8"}):
            config = Config()
            assert config.DOWNLOAD_CONCURRENCY == 8
    
    def test_chunk_size_override(self):
        """Test that CHUNK_SIZE can be overridden."""
        with patch.dict(os.environ, {"CHIRPS_CHUNK_SIZE": "16777216"}):  # 16MB
            config = Config()
            assert config.CHUNK_SIZE == 16777216
    
    def test_timeout_override(self):
        """Test that TIMEOUT_SECONDS can be overridden."""
        with patch.dict(os.environ, {"CHIRPS_TIMEOUT_SECONDS": "600"}):
            config = Config()
            assert config.TIMEOUT_SECONDS == 600
    
    def test_max_retries_override(self):
        """Test that MAX_RETRIES can be overridden."""
        with patch.dict(os.environ, {"CHIRPS_MAX_RETRIES": "5"}):
            config = Config()
            assert config.MAX_RETRIES == 5
    
    def test_bootstrap_start_date_override(self):
        """Test that BOOTSTRAP_START_DATE can be overridden."""
        with patch.dict(os.environ, {"CHIRPS_BOOTSTRAP_START_DATE": "2021-06-01"}):
            config = Config()
            assert config.BOOTSTRAP_START_DATE == date(2021, 6, 1)
    
    def test_bootstrap_end_date_override(self):
        """Test that BOOTSTRAP_END_DATE can be overridden."""
        with patch.dict(os.environ, {"CHIRPS_BOOTSTRAP_END_DATE": "2021-12-31"}):
            config = Config()
            assert config.BOOTSTRAP_END_DATE == date(2021, 12, 31)
    
    def test_chirps_base_url_override(self):
        """Test that CHIRPS_BASE_URL can be overridden."""
        custom_url = "https://custom.example.com/chirps"
        with patch.dict(os.environ, {"CHIRPS_BASE_URL": custom_url}):
            config = Config()
            assert config.CHIRPS_BASE_URL == custom_url


class TestURLConstruction:
    """Test CHIRPS URL construction."""
    
    def test_get_chirps_url_format(self):
        """Test that get_chirps_url constructs valid URLs."""
        config = Config()
        url = config.get_chirps_url(2025, 1, 15)
        
        assert "2025" in url
        assert "01" in url
        assert "15" in url
        assert url.endswith(".tif")
    
    def test_get_chirps_url_contains_base(self):
        """Test that constructed URL contains base URL."""
        config = Config()
        url = config.get_chirps_url(2020, 5, 25)
        assert config.CHIRPS_BASE_URL in url
    
    def test_get_chirps_url_month_padding(self):
        """Test that month is zero-padded in URL."""
        config = Config()
        url = config.get_chirps_url(2020, 3, 5)
        assert ".03." in url
    
    def test_get_chirps_url_day_padding(self):
        """Test that day is zero-padded in URL."""
        config = Config()
        url = config.get_chirps_url(2020, 10, 7)
        assert ".07." in url


class TestLoggerName:
    """Test logger name generation."""
    
    def test_get_logger_name_basic(self):
        """Test basic logger name generation."""
        config = Config()
        logger_name = config.get_logger_name("src.download.chirps_downloader")
        assert logger_name == "chirps_zarr.download.chirps_downloader"
    
    def test_get_logger_name_removes_src_prefix(self):
        """Test that 'src.' prefix is removed from logger names."""
        config = Config()
        logger_name = config.get_logger_name("src.utils.logging")
        assert not logger_name.startswith("chirps_zarr.src")
        assert logger_name == "chirps_zarr.utils.logging"
    
    def test_get_logger_name_without_src_prefix(self):
        """Test logger name generation without 'src.' prefix."""
        config = Config()
        logger_name = config.get_logger_name("convert.tiff_to_zarr")
        assert logger_name == "chirps_zarr.convert.tiff_to_zarr"


class TestMetadataConfig:
    """Test metadata configuration loading and saving."""
    
    def test_default_metadata_structure(self):
        """Test that default metadata has expected structure."""
        config = Config()
        metadata = config._get_default_metadata()
        
        assert "title" in metadata
        assert "institution" in metadata
        assert "source" in metadata
        assert "Conventions" in metadata
    
    def test_load_metadata_config_default(self, tmp_path):
        """Test loading metadata when config file doesn't exist."""
        with patch.dict(os.environ, {"CHIRPS_BASE_DIR": str(tmp_path)}):
            config = Config()
            # Remove the metadata file if it exists
            if config.METADATA_CONFIG_PATH.exists():
                config.METADATA_CONFIG_PATH.unlink()
            
            metadata = config.load_metadata_config()
            assert "title" in metadata
            assert "CHIRPS" in metadata["title"]
    
    def test_save_default_metadata_config(self, tmp_path):
        """Test saving default metadata configuration."""
        with patch.dict(os.environ, {"CHIRPS_BASE_DIR": str(tmp_path)}):
            config = Config()
            config.save_default_metadata_config()
            
            assert config.METADATA_CONFIG_PATH.exists()
            
            # Verify it's valid JSON
            with open(config.METADATA_CONFIG_PATH, 'r') as f:
                metadata = json.load(f)
            
            assert "title" in metadata
    
    def test_load_custom_metadata_config(self, tmp_path):
        """Test loading custom metadata configuration."""
        custom_metadata = {
            "title": "Custom Title",
            "custom_field": "custom_value"
        }
        
        metadata_path = tmp_path / "config" / "metadata.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(metadata_path, 'w') as f:
            json.dump(custom_metadata, f)
        
        with patch.dict(os.environ, {
            "CHIRPS_BASE_DIR": str(tmp_path),
            "CHIRPS_METADATA_CONFIG": "config/metadata.json"
        }):
            config = Config()
            loaded_metadata = config.load_metadata_config()
            
            assert loaded_metadata["title"] == "Custom Title"
            assert loaded_metadata["custom_field"] == "custom_value"


class TestSingletonPattern:
    """Test the singleton configuration pattern."""
    
    def test_get_config_returns_config(self):
        """Test that get_config returns a Config instance."""
        config = get_config()
        assert isinstance(config, Config)
    
    def test_get_config_returns_same_instance(self):
        """Test that get_config returns the same instance each time."""
        config1 = get_config()
        config2 = get_config()
        assert config1 is config2
