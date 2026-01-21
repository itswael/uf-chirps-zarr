"""
Raster validation and cleaning for CHIRPS GeoTIFF files.

Enforces spatial, temporal, and format invariants to ensure data quality
before conversion to Zarr format.
"""

from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import rasterio
from rasterio.crs import CRS
import numpy as np

from src.config import Config
from src.utils.logging import AuditLogger, setup_logger


class ValidationError(Exception):
    """Exception raised when raster validation fails."""
    pass


class RasterValidator:
    """
    Validator for CHIRPS v3.0 GeoTIFF files.
    
    Validates:
    - File existence and readability
    - CRS (Coordinate Reference System)
    - Spatial resolution and grid alignment
    - Data value ranges
    - Fill values and missing data
    - Temporal consistency
    """
    
    # Expected CHIRPS v3.0 specifications
    EXPECTED_CRS = "EPSG:4326"  # WGS84
    EXPECTED_RESOLUTION = 0.05  # 0.05 degrees
    EXPECTED_NODATA = -99.0
    VALID_PRECIP_MIN = 0.0
    VALID_PRECIP_MAX = 2000.0  # mm/day (extreme rainfall threshold)
    
    def __init__(
        self,
        config: Config,
        audit_logger: Optional[AuditLogger] = None,
        strict: bool = True
    ):
        """
        Initialize the raster validator.
        
        Args:
            config: Application configuration
            audit_logger: Optional audit logger
            strict: Whether to enforce strict validation (raise on errors)
        """
        self.config = config
        self.logger = setup_logger(
            self.config.get_logger_name(__name__),
            log_dir=self.config.BASE_DIR / "logs"
        )
        self.audit_logger = audit_logger
        self.strict = strict
    
    def validate_file(
        self,
        file_path: Path,
        expected_date: Optional[date] = None,
        reference_metadata: Optional[Dict] = None
    ) -> Tuple[bool, List[str], Dict]:
        """
        Validate a CHIRPS GeoTIFF file.
        
        Args:
            file_path: Path to the GeoTIFF file
            expected_date: Expected date for temporal validation
            reference_metadata: Optional reference metadata for consistency checks
            
        Returns:
            Tuple of (is_valid, errors, metadata)
            - is_valid: Whether validation passed
            - errors: List of validation error messages
            - metadata: Extracted raster metadata
        """
        errors = []
        metadata = {}
        
        date_str = expected_date.strftime("%Y-%m-%d") if expected_date else "unknown"
        
        try:
            # Check file existence
            if not file_path.exists():
                errors.append(f"File does not exist: {file_path}")
                return False, errors, metadata
            
            # Check file size
            file_size = file_path.stat().st_size
            if file_size == 0:
                errors.append(f"File is empty: {file_path}")
                return False, errors, metadata
            
            metadata["file_size_bytes"] = file_size
            
            # Open and validate raster
            with rasterio.open(file_path) as src:
                # Extract metadata
                metadata.update({
                    "width": src.width,
                    "height": src.height,
                    "crs": str(src.crs),
                    "bounds": src.bounds,
                    "transform": list(src.transform)[:6],
                    "dtype": str(src.dtypes[0]),
                    "nodata": src.nodata,
                    "count": src.count
                })
                
                # Validate CRS
                if str(src.crs) != self.EXPECTED_CRS:
                    errors.append(
                        f"Unexpected CRS: {src.crs}, expected {self.EXPECTED_CRS}"
                    )
                
                # Validate band count
                if src.count != 1:
                    errors.append(
                        f"Unexpected band count: {src.count}, expected 1"
                    )
                
                # Validate resolution
                resolution_x = abs(src.transform[0])
                resolution_y = abs(src.transform[4])
                
                if not np.isclose(resolution_x, self.EXPECTED_RESOLUTION, rtol=1e-5):
                    errors.append(
                        f"Unexpected X resolution: {resolution_x}, "
                        f"expected {self.EXPECTED_RESOLUTION}"
                    )
                
                if not np.isclose(resolution_y, self.EXPECTED_RESOLUTION, rtol=1e-5):
                    errors.append(
                        f"Unexpected Y resolution: {resolution_y}, "
                        f"expected {self.EXPECTED_RESOLUTION}"
                    )
                
                # Validate against reference metadata (for spatial consistency)
                if reference_metadata:
                    if metadata["width"] != reference_metadata.get("width"):
                        errors.append(
                            f"Width mismatch: {metadata['width']} vs "
                            f"{reference_metadata.get('width')}"
                        )
                    
                    if metadata["height"] != reference_metadata.get("height"):
                        errors.append(
                            f"Height mismatch: {metadata['height']} vs "
                            f"{reference_metadata.get('height')}"
                        )
                    
                    if metadata["crs"] != reference_metadata.get("crs"):
                        errors.append(
                            f"CRS mismatch: {metadata['crs']} vs "
                            f"{reference_metadata.get('crs')}"
                        )
                
                # Read and validate data
                try:
                    data = src.read(1)
                    
                    # Check for all NaN or NoData
                    valid_mask = data != src.nodata
                    if src.nodata is not None:
                        valid_mask &= ~np.isnan(data)
                    
                    valid_count = np.sum(valid_mask)
                    total_count = data.size
                    
                    metadata["valid_pixels"] = int(valid_count)
                    metadata["total_pixels"] = int(total_count)
                    metadata["valid_percentage"] = (valid_count / total_count * 100) if total_count > 0 else 0
                    
                    if valid_count == 0:
                        errors.append("No valid data pixels found (all NoData or NaN)")
                    
                    # Validate value ranges for valid data
                    if valid_count > 0:
                        valid_data = data[valid_mask]
                        
                        metadata["min_value"] = float(np.min(valid_data))
                        metadata["max_value"] = float(np.max(valid_data))
                        metadata["mean_value"] = float(np.mean(valid_data))
                        metadata["std_value"] = float(np.std(valid_data))
                        
                        # Check for negative precipitation (except fill value)
                        negative_mask = valid_data < self.VALID_PRECIP_MIN
                        if np.any(negative_mask):
                            negative_count = np.sum(negative_mask)
                            errors.append(
                                f"Found {negative_count} negative precipitation values "
                                f"(min: {np.min(valid_data)})"
                            )
                        
                        # Check for unrealistic high values
                        if metadata["max_value"] > self.VALID_PRECIP_MAX:
                            errors.append(
                                f"Unrealistic max precipitation: {metadata['max_value']} mm/day "
                                f"(threshold: {self.VALID_PRECIP_MAX})"
                            )
                
                except Exception as e:
                    errors.append(f"Error reading raster data: {str(e)}")
        
        except rasterio.errors.RasterioIOError as e:
            errors.append(f"Failed to open raster file: {str(e)}")
        except Exception as e:
            errors.append(f"Unexpected validation error: {str(e)}")
        
        # Determine validation result
        is_valid = len(errors) == 0
        
        # Log results
        if is_valid:
            self.logger.info(
                f"Validation passed for {date_str}: {file_path.name} "
                f"({metadata.get('valid_percentage', 0):.1f}% valid pixels)"
            )
        else:
            self.logger.warning(
                f"Validation failed for {date_str}: {file_path.name} "
                f"with {len(errors)} error(s)"
            )
            for error in errors:
                self.logger.warning(f"  - {error}")
        
        # Audit log
        if self.audit_logger:
            self.audit_logger.log_validation_result(
                date_str, str(file_path), is_valid, errors if not is_valid else None, metadata
            )
        
        # Raise exception if strict mode and validation failed
        if self.strict and not is_valid:
            raise ValidationError(
                f"Validation failed for {file_path}: {'; '.join(errors)}"
            )
        
        return is_valid, errors, metadata
    
    def extract_reference_metadata(self, file_path: Path) -> Dict:
        """
        Extract reference metadata from a GeoTIFF file.
        
        This metadata is used to validate spatial consistency across files.
        
        Args:
            file_path: Path to the reference GeoTIFF
            
        Returns:
            Dictionary of reference metadata
        """
        with rasterio.open(file_path) as src:
            return {
                "width": src.width,
                "height": src.height,
                "crs": str(src.crs),
                "bounds": src.bounds,
                "transform": src.transform,
                "resolution": (abs(src.transform[0]), abs(src.transform[4]))
            }
    
    def validate_temporal_sequence(
        self,
        file_paths: List[Path],
        expected_dates: List[date]
    ) -> Tuple[bool, List[str]]:
        """
        Validate that files form a proper temporal sequence.
        
        Args:
            file_paths: List of file paths in temporal order
            expected_dates: List of expected dates in temporal order
            
        Returns:
            Tuple of (is_valid, errors)
        """
        errors = []
        
        if len(file_paths) != len(expected_dates):
            errors.append(
                f"Mismatch between file count ({len(file_paths)}) "
                f"and date count ({len(expected_dates)})"
            )
            return False, errors
        
        # Check temporal ordering
        for i in range(1, len(expected_dates)):
            if expected_dates[i] <= expected_dates[i-1]:
                errors.append(
                    f"Dates not in ascending order: {expected_dates[i-1]} -> {expected_dates[i]}"
                )
        
        # Check for gaps
        for i in range(1, len(expected_dates)):
            expected_delta = (expected_dates[i] - expected_dates[i-1]).days
            if expected_delta != 1:
                errors.append(
                    f"Gap in daily sequence: {expected_dates[i-1]} to {expected_dates[i]} "
                    f"({expected_delta} days)"
                )
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info(
                f"Temporal sequence validated: {len(file_paths)} files from "
                f"{expected_dates[0]} to {expected_dates[-1]}"
            )
        else:
            self.logger.warning(
                f"Temporal sequence validation failed with {len(errors)} error(s)"
            )
        
        return is_valid, errors
    
    def check_data_completeness(self, metadata: Dict, min_valid_percentage: float = 80.0) -> bool:
        """
        Check if raster has sufficient valid data.
        
        Args:
            metadata: Raster metadata from validation
            min_valid_percentage: Minimum percentage of valid pixels required
            
        Returns:
            True if data is sufficiently complete
        """
        valid_pct = metadata.get("valid_percentage", 0)
        is_complete = valid_pct >= min_valid_percentage
        
        if not is_complete:
            self.logger.warning(
                f"Data completeness check failed: {valid_pct:.1f}% valid "
                f"(threshold: {min_valid_percentage}%)"
            )
        
        return is_complete
