"""
Bootstrap ingestion orchestrator for CHIRPS v3.0 data.

Coordinates the complete bootstrap ingestion workflow:
1. Verify Zarr store doesn't exist
2. Enumerate historical dates
3. Download GeoTIFFs concurrently
4. Validate files sequentially
5. Initialize Zarr store with first file
6. Process and append remaining files in order
7. Finalize metadata
"""

import time
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

from src.config import Config, get_config
from src.download.chirps_downloader import CHIRPSDownloader
from src.preprocess.raster_cleaner import RasterValidator, ValidationError
from src.convert.tiff_to_zarr import TIFFToZarrConverter, ZarrConversionError
from src.utils.logging import AuditLogger, setup_logger


class BootstrapOrchestrationError(Exception):
    """Exception raised during bootstrap orchestration."""
    pass


class BootstrapOrchestrator:
    """
    Orchestrator for bootstrap ingestion of CHIRPS data.
    
    Implements the workflow defined in the Technical Design Document:
    - One-time historical backfill
    - Parallel downloads followed by sequential processing
    - Zarr store initialization and ordered appending
    - Comprehensive logging and error handling
    """
    
    def __init__(
        self,
        config: Optional[Config] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ):
        """
        Initialize the bootstrap orchestrator.
        
        Args:
            config: Application configuration (uses singleton if None)
            start_date: Override bootstrap start date
            end_date: Override bootstrap end date
        """
        self.config = config or get_config()
        
        # Use provided dates or fall back to config
        self.start_date = start_date or self.config.BOOTSTRAP_START_DATE
        self.end_date = end_date or self.config.BOOTSTRAP_END_DATE
        
        # Validate date range
        if self.end_date < self.start_date:
            raise BootstrapOrchestrationError(
                f"End date {self.end_date} is before start date {self.start_date}"
            )
        
        # Setup logging
        self.logger = setup_logger(
            self.config.get_logger_name(__name__),
            log_dir=self.config.BASE_DIR / "logs"
        )
        
        # Setup audit logging
        self.audit_logger = AuditLogger(
            log_dir=self.config.BASE_DIR / "logs",
            name="bootstrap"
        )
        
        # Initialize components
        self.downloader = CHIRPSDownloader(
            self.config,
            audit_logger=self.audit_logger,
            skip_existing=True
        )
        
        self.validator = RasterValidator(
            self.config,
            audit_logger=self.audit_logger,
            strict=False  # Don't fail on validation warnings
        )
        
        self.converter = TIFFToZarrConverter(
            self.config,
            audit_logger=self.audit_logger
        )
        
        self.zarr_path = self.config.ZARR_STORE_PATH
    
    def _generate_date_list(self) -> List[date]:
        """
        Generate list of dates for bootstrap ingestion.
        
        Returns:
            List of dates from start_date to end_date (inclusive)
        """
        dates = []
        current = self.start_date
        while current <= self.end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates
    
    def _verify_zarr_not_exists(self) -> None:
        """
        Verify that the Zarr store doesn't already exist.
        
        Raises:
            BootstrapOrchestrationError: If store exists
        """
        if self.zarr_path.exists():
            raise BootstrapOrchestrationError(
                f"Zarr store already exists: {self.zarr_path}. "
                "Delete it first or use incremental mode."
            )
        
        self.logger.info(f"Verified Zarr path is available: {self.zarr_path}")
    
    def run(self, skip_download: bool = False) -> Tuple[int, int, int]:
        """
        Execute the complete bootstrap ingestion workflow.
        
        Args:
            skip_download: Skip download phase (assume files already exist)
            
        Returns:
            Tuple of (total_days, successful, failed)
            
        Raises:
            BootstrapOrchestrationError: If critical error occurs
        """
        workflow_start = time.time()
        
        # Generate date list
        dates = self._generate_date_list()
        total_days = len(dates)
        
        self.logger.info("=" * 80)
        self.logger.info("STARTING BOOTSTRAP INGESTION")
        self.logger.info("=" * 80)
        self.logger.info(f"Date range: {self.start_date} to {self.end_date}")
        self.logger.info(f"Total days: {total_days}")
        self.logger.info(f"Zarr path: {self.zarr_path}")
        self.logger.info("=" * 80)
        
        # Audit log
        self.audit_logger.log_bootstrap_start(
            self.start_date.isoformat(),
            self.end_date.isoformat(),
            total_days
        )
        
        try:
            # Step 1: Verify Zarr store doesn't exist
            self._verify_zarr_not_exists()
            
            # Step 2: Download phase (parallel)
            if not skip_download:
                download_files, download_failures = self._download_phase(dates)
                self.logger.info(
                    f"Download phase complete: {len(download_files)} files, "
                    f"{len(download_failures)} failures"
                )
            else:
                self.logger.info("Skipping download phase (skip_download=True)")
                download_files, download_failures = self._collect_existing_files(dates)
            
            # Step 3: Ordered processing phase (sequential)
            successful, failed = self._processing_phase(dates)
            
            # Step 4: Finalize
            if successful > 0:
                self._finalize_zarr()
            
            workflow_duration = time.time() - workflow_start
            
            # Final summary
            self.logger.info("=" * 80)
            self.logger.info("BOOTSTRAP INGESTION COMPLETE")
            self.logger.info("=" * 80)
            self.logger.info(f"Total days: {total_days}")
            self.logger.info(f"Successfully ingested: {successful}")
            self.logger.info(f"Failed: {failed}")
            self.logger.info(f"Duration: {workflow_duration:.2f}s ({workflow_duration/60:.2f} minutes)")
            self.logger.info("=" * 80)
            
            # Audit log
            self.audit_logger.log_bootstrap_complete(
                self.start_date.isoformat(),
                self.end_date.isoformat(),
                total_days,
                successful,
                failed,
                workflow_duration
            )
            
            return total_days, successful, failed
            
        except Exception as e:
            self.logger.error(f"Bootstrap ingestion failed: {e}", exc_info=True)
            raise BootstrapOrchestrationError(f"Bootstrap failed: {e}")
        finally:
            # Cleanup
            self.downloader.cleanup_session()
    
    def _download_phase(self, dates: List[date]) -> Tuple[List[Path], List[Tuple[date, str]]]:
        """
        Execute parallel download phase.
        
        Args:
            dates: List of dates to download
            
        Returns:
            Tuple of (successful_files, failed_downloads)
        """
        self.logger.info(f"Starting download phase for {len(dates)} files...")
        
        download_start = time.time()
        
        successful_files, failed_downloads = self.downloader.download_date_range(
            self.start_date,
            self.end_date,
            max_workers=self.config.DOWNLOAD_CONCURRENCY
        )
        
        download_duration = time.time() - download_start
        
        self.logger.info(
            f"Download phase completed in {download_duration:.2f}s: "
            f"{len(successful_files)} successful, {len(failed_downloads)} failed"
        )
        
        if failed_downloads:
            self.logger.warning(f"Failed downloads ({len(failed_downloads)}):")
            for failed_date, error in failed_downloads[:10]:  # Show first 10
                self.logger.warning(f"  {failed_date}: {error}")
            if len(failed_downloads) > 10:
                self.logger.warning(f"  ... and {len(failed_downloads) - 10} more")
        
        return successful_files, failed_downloads
    
    def _collect_existing_files(self, dates: List[date]) -> Tuple[List[Path], List[Tuple[date, str]]]:
        """
        Collect existing downloaded files without downloading.
        
        Args:
            dates: List of dates to check
            
        Returns:
            Tuple of (existing_files, missing_files)
        """
        self.logger.info("Collecting existing files...")
        
        existing_files = []
        missing_files = []
        
        for download_date in dates:
            filename = f"chirps-v3.0.rnl.{download_date.strftime('%Y.%m.%d')}.tif"
            file_path = self.config.RAW_DIR / str(download_date.year) / filename
            
            if file_path.exists() and file_path.stat().st_size > 0:
                existing_files.append(file_path)
            else:
                missing_files.append((download_date, "File not found"))
        
        self.logger.info(
            f"Found {len(existing_files)} existing files, "
            f"{len(missing_files)} missing"
        )
        
        return existing_files, missing_files
    
    def _processing_phase(self, dates: List[date]) -> Tuple[int, int]:
        """
        Execute sequential processing and Zarr appending.
        
        Args:
            dates: List of dates in temporal order
            
        Returns:
            Tuple of (successful_count, failed_count)
        """
        self.logger.info("Starting ordered processing phase...")
        
        processing_start = time.time()
        successful = 0
        failed = 0
        reference_metadata = None
        zarr_initialized = False
        
        for i, processing_date in enumerate(dates):
            try:
                # Construct file path
                filename = f"chirps-v3.0.rnl.{processing_date.strftime('%Y.%m.%d')}.tif"
                file_path = self.config.RAW_DIR / str(processing_date.year) / filename
                
                # Check file exists
                if not file_path.exists():
                    self.logger.warning(f"Skipping {processing_date}: file not found")
                    failed += 1
                    continue
                
                # Validate file
                is_valid, errors, metadata = self.validator.validate_file(
                    file_path,
                    expected_date=processing_date,
                    reference_metadata=reference_metadata
                )
                
                if not is_valid:
                    self.logger.warning(
                        f"Skipping {processing_date}: validation failed ({len(errors)} errors)"
                    )
                    failed += 1
                    continue
                
                # Store reference metadata from first valid file
                if reference_metadata is None:
                    reference_metadata = metadata
                    self.logger.info("Established reference metadata from first valid file")
                
                # Convert to xarray Dataset
                dataset = self.converter.tiff_to_dataset(file_path, processing_date)
                
                # Initialize Zarr store with first dataset
                if not zarr_initialized:
                    self.converter.initialize_zarr_store(
                        dataset,
                        self.zarr_path,
                        processing_date
                    )
                    zarr_initialized = True
                    self.logger.info(f"Zarr store initialized with date: {processing_date}")
                else:
                    # Append to existing Zarr store
                    self.converter.append_to_zarr(
                        dataset,
                        self.zarr_path,
                        processing_date
                    )
                
                successful += 1
                
                # Log progress
                if (i + 1) % 10 == 0 or (i + 1) == len(dates):
                    self.logger.info(
                        f"Processing progress: {i + 1}/{len(dates)} "
                        f"({successful} successful, {failed} failed)"
                    )
                
            except (ValidationError, ZarrConversionError) as e:
                self.logger.error(f"Failed to process {processing_date}: {e}")
                failed += 1
            except Exception as e:
                self.logger.error(
                    f"Unexpected error processing {processing_date}: {e}",
                    exc_info=True
                )
                failed += 1
        
        processing_duration = time.time() - processing_start
        
        self.logger.info(
            f"Processing phase completed in {processing_duration:.2f}s: "
            f"{successful} successful, {failed} failed"
        )
        
        return successful, failed
    
    def _finalize_zarr(self) -> None:
        """Finalize the Zarr store with final metadata."""
        self.logger.info("Finalizing Zarr store...")
        
        try:
            self.converter.finalize_zarr_store(self.zarr_path, self.end_date)
            self.logger.info("Zarr store finalized successfully")
        except Exception as e:
            self.logger.error(f"Failed to finalize Zarr store: {e}")
            # Don't raise - data is already written
