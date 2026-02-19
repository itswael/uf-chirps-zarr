"""
Incremental ingestion orchestrator for CHIRPS v3.0 data.

Coordinates the incremental workflow:
1. Read latest date from existing Zarr store
2. Compute next expected date
3. Download consecutive available days incrementally
4. Validate each file sequentially
5. Convert and append to Zarr store
6. Update metadata
7. Generate summary report
8. Send email notification with execution summary

Implements the workflow defined in Section 8 of the Technical Design Document.
Addresses GitHub Issue #20: Incremental orchestrator
"""

import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.config import Config, get_config
from src.download.chirps_downloader import CHIRPSDownloader
from src.preprocess.raster_cleaner import RasterValidator, ValidationError
from src.convert.tiff_to_zarr import TIFFToZarrConverter, ZarrConversionError
from src.utils.logging import AuditLogger, setup_logger
from src.utils.zarr_state import ZarrStateManager
from src.utils.email_notifier import EmailNotifier


class IncrementalIngestionError(Exception):
    """Exception raised during incremental ingestion."""
    pass


class IncrementalOrchestrator:
    """
    Orchestrator for incremental ingestion of CHIRPS data.
    
    Implements the incremental workflow:
    - Determines next expected date from existing Zarr store
    - Downloads newly available consecutive days
    - Validates and appends each day sequentially
    - Maintains idempotency (prevents duplicate ingestion)
    - Updates metadata with latest ingested date
    - Generates comprehensive summary reports
    
    Design principles:
    - Simplicity over throughput (one file at a time)
    - Correctness over speed
    - Idempotent operations (safe to re-run)
    - Clear failure handling with detailed logging
    """
    
    def __init__(
        self,
        config: Optional[Config] = None,
        max_days_per_run: Optional[int] = None
    ):
        """
        Initialize the incremental orchestrator.
        
        Args:
            config: Application configuration (uses singleton if None)
            max_days_per_run: Maximum number of days to ingest in one run.
                            None = unlimited (downloads up to today)
                            Use config.INCREMENTAL_MAX_DAYS_PER_RUN if not specified
        """
        self.config = config or get_config()
        # Use provided value, else config value, else None (unlimited)
        self.max_days_per_run = (
            max_days_per_run 
            if max_days_per_run is not None 
            else self.config.INCREMENTAL_MAX_DAYS_PER_RUN
        )
        
        # Setup logging
        self.logger = setup_logger(
            self.config.get_logger_name(__name__),
            log_dir=self.config.BASE_DIR / "logs"
        )
        
        # Setup audit logging
        self.audit_logger = AuditLogger(
            log_dir=self.config.BASE_DIR / "logs",
            name="incremental"
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
        self.state_manager = ZarrStateManager(self.zarr_path, self.config)
        
        # Initialize email notifier
        self.email_notifier = EmailNotifier()
        
        # Track ingestion results
        self.summary: Dict = {}
        self.error_messages: List[str] = []
        self.gaps_detected: List[str] = []
    
    def _verify_zarr_exists(self) -> None:
        """
        Verify that the Zarr store exists and is valid.
        
        Raises:
            IncrementalIngestionError: If store doesn't exist or is invalid
        """
        if not self.state_manager.exists():
            raise IncrementalIngestionError(
                f"Zarr store does not exist: {self.zarr_path}. "
                "Run bootstrap ingestion first."
            )
        
        if not self.state_manager.is_bootstrap_complete():
            self.logger.warning(
                "Bootstrap may not be complete (bootstrap_complete flag not set). "
                "Proceeding with incremental ingestion anyway."
            )
        
        self.logger.info(f"Zarr store verified: {self.zarr_path}")
    
    def _determine_next_date(self) -> Optional[date]:
        """
        Determine the next expected date for incremental ingestion.
        
        Returns:
            Next date to ingest, or None if unable to determine
        """
        next_date = self.state_manager.get_next_expected_date()
        
        if next_date is None:
            raise IncrementalIngestionError(
                "Unable to determine next expected date. "
                "Zarr store may be empty or corrupted."
            )
        
        self.logger.info(f"Next expected date: {next_date}")
        return next_date
    
    def run(
        self,
        force_date: Optional[date] = None,
        dry_run: bool = False
    ) -> Dict:
        """
        Execute the incremental ingestion workflow.
        
        Workflow steps (per TDD Section 8):
        1. Verify Zarr store exists
        2. Determine next expected date
        3. Check availability and download consecutive days
        4. For each day sequentially:
           a. Validate file integrity
           b. Validate CRS and spatial consistency
           c. Convert to xarray Dataset
           d. Check for duplicate (idempotency)
           e. Append to Zarr store
           f. Update metadata
        5. Generate summary report
        6. Send email notification
        
        Args:
            force_date: Force starting from specific date (overrides automatic detection)
            dry_run: If True, check availability but don't download/ingest
        
        Returns:
            Dictionary with ingestion summary
        
        Raises:
            IncrementalIngestionError: If critical error occurs
        """
        workflow_start = time.time()
        workflow_start_datetime = datetime.now()
        self.error_messages.clear()  # Reset error tracking
        self.gaps_detected.clear()  # Reset gap tracking
        
        self.logger.info("=" * 80)
        self.logger.info("STARTING INCREMENTAL INGESTION")
        self.logger.info("=" * 80)
        
        try:
            # Step 1: Verify Zarr store exists
            self._verify_zarr_exists()
            
            # Get current state
            latest_date = self.state_manager.get_latest_date()
            date_count = self.state_manager.get_date_count()
            
            self.logger.info(f"Current Zarr state:")
            self.logger.info(f"  Latest date: {latest_date}")
            self.logger.info(f"  Total dates: {date_count}")
            
            # Step 2: Determine next expected date
            if force_date:
                next_date = force_date
                self.logger.info(f"Using forced start date: {next_date}")
            else:
                next_date = self._determine_next_date()
            
            # Audit log
            self.audit_logger.log_event(
                "incremental_start",
                {
                    "next_expected_date": next_date.isoformat(),
                    "max_days_per_run": self.max_days_per_run,
                    "dry_run": dry_run
                }
            )
            
            if dry_run:
                self.logger.info("DRY RUN MODE - No files will be downloaded or ingested")
                workflow_duration = time.time() - workflow_start
                workflow_end_datetime = datetime.now()
                
                # Generate summary for dry-run
                summary = self._generate_summary(
                    next_date, 0, 0, 0, workflow_duration, dry_run=True
                )
                
                # Send email notification for dry-run
                self._send_email_notification(
                    success=True,
                    start_time=workflow_start_datetime,
                    end_time=workflow_end_datetime,
                    dates_checked=0,
                    new_files_found=0,
                    files_ingested=0,
                    files_failed=0,
                    next_expected_date=summary.get('next_expected_date')
                )
                
                return summary
            
            # Step 3: Download phase (incremental, consecutive days)
            downloaded_files, download_failures, last_download_date = self._download_phase(next_date)
            
            if len(downloaded_files) == 0:
                self.logger.info("No new files available for ingestion.")
                workflow_duration = time.time() - workflow_start
                workflow_end_datetime = datetime.now()
                
                # Generate summary
                summary = self._generate_summary(
                    next_date, 0, 0, 0, workflow_duration
                )
                
                # Send email notification (no new files)
                self._send_email_notification(
                    success=True,
                    start_time=workflow_start_datetime,
                    end_time=workflow_end_datetime,
                    dates_checked=1,  # We checked at least one date
                    new_files_found=0,
                    files_ingested=0,
                    files_failed=0,
                    next_expected_date=summary.get('next_expected_date')
                )
                
                return summary
            
            self.logger.info(
                f"Downloaded {len(downloaded_files)} files "
                f"(up to {last_download_date})"
            )
            
            # Step 4: Processing phase (sequential validation and append)
            successful, failed, skipped = self._processing_phase(next_date, downloaded_files)
            
            # Step 5: Update final metadata
            if successful > 0:
                self._update_final_metadata()
            
            workflow_duration = time.time() - workflow_start
            workflow_end_datetime = datetime.now()
            
            # Step 6: Generate summary
            summary = self._generate_summary(
                next_date,
                successful,
                failed,
                skipped,
                workflow_duration,
                last_download_date
            )
            
            # Step 7: Send email notification
            self._send_email_notification(
                success=True,
                start_time=workflow_start_datetime,
                end_time=workflow_end_datetime,
                dates_checked=len(downloaded_files) if downloaded_files else 0,
                new_files_found=len(downloaded_files) if downloaded_files else 0,
                files_ingested=successful,
                files_failed=failed,
                next_expected_date=summary.get('next_expected_date')
            )
            
            return summary
            
        except Exception as e:
            workflow_end_datetime = datetime.now()
            self.error_messages.append(f"Critical error: {str(e)}")
            self.logger.error(f"Incremental ingestion failed: {e}", exc_info=True)
            
            # Send failure email notification
            self._send_email_notification(
                success=False,
                start_time=workflow_start_datetime,
                end_time=workflow_end_datetime,
                dates_checked=0,
                new_files_found=0,
                files_ingested=0,
                files_failed=1
            )
            
            raise IncrementalIngestionError(f"Incremental ingestion failed: {e}")
        finally:
            # Cleanup
            self.downloader.cleanup_session()
            # Invalidate cache since we modified the Zarr store
            self.state_manager.invalidate_cache()
    
    def _download_phase(
        self,
        start_date: date
    ) -> Tuple[List[Path], List[Tuple[date, str]], Optional[date]]:
        """
        Execute incremental download phase.
        
        Downloads consecutive days starting from start_date until:
        - Files are no longer available (404)
        - max_days_per_run is reached (if set)
        - A download failure occurs
        
        Args:
            start_date: First date to download
        
        Returns:
            Tuple of (successful_files, failed_downloads, last_successful_date)
        """
        # Calculate max days to download
        if self.max_days_per_run is None:
            # No limit - download up to today
            today = date.today()
            days_to_download = (today - start_date).days + 1
            limit_description = f"up to today ({today})"
        else:
            days_to_download = self.max_days_per_run
            limit_description = f"max {self.max_days_per_run} days"
        
        self.logger.info(
            f"Starting incremental download from {start_date}, {limit_description}"
        )
        
        download_start = time.time()
        
        successful_files, failed_downloads, last_date = self.downloader.download_incremental(
            start_date=start_date,
            max_consecutive_days=days_to_download,
            stop_on_missing=True  # Stop when file not available
        )
        
        download_duration = time.time() - download_start
        
        self.logger.info(
            f"Download phase completed in {download_duration:.2f}s: "
            f"{len(successful_files)} successful, {len(failed_downloads)} failed"
        )
        
        if failed_downloads:
            self.logger.warning(f"Failed downloads ({len(failed_downloads)}):")
            for failed_date, error in failed_downloads:
                self.logger.warning(f"  {failed_date}: {error}")
        
        return successful_files, failed_downloads, last_date
    
    def _processing_phase(
        self,
        start_date: date,
        downloaded_files: List[Path]
    ) -> Tuple[int, int, int]:
        """
        Execute sequential processing and Zarr appending.
        
        Processes each file in order:
        1. Validate file
        2. Check if date already exists (idempotency)
        3. Convert to xarray Dataset
        4. Append to Zarr store
        5. Log success/failure
        
        Args:
            start_date: First date being processed
            downloaded_files: List of downloaded file paths
        
        Returns:
            Tuple of (successful_count, failed_count, skipped_count)
        """
        self.logger.info(f"Starting sequential processing of {len(downloaded_files)} files...")
        
        processing_start = time.time()
        successful = 0
        failed = 0
        skipped = 0
        reference_metadata = None
        
        # Generate expected dates based on downloaded files
        current_date = start_date
        
        for i, file_path in enumerate(downloaded_files):
            processing_date = current_date
            
            try:
                self.logger.info(
                    f"Processing {i + 1}/{len(downloaded_files)}: {processing_date}"
                )
                
                # Step 1: Verify file exists
                if not file_path.exists():
                    gap_msg = f"{processing_date.isoformat()}"
                    self.logger.warning(f"Skipping {processing_date}: file not found")
                    self.gaps_detected.append(gap_msg)
                    failed += 1
                    current_date += timedelta(days=1)
                    continue
                
                # Step 2: Idempotency check - does date already exist?
                if self.converter.check_date_exists(self.zarr_path, processing_date):
                    self.logger.info(
                        f"Skipping {processing_date}: already exists in Zarr (idempotency)"
                    )
                    skipped += 1
                    current_date += timedelta(days=1)
                    continue
                
                # Step 3: Validate file
                is_valid, errors, metadata = self.validator.validate_file(
                    file_path,
                    expected_date=processing_date,
                    reference_metadata=reference_metadata
                )
                
                if not is_valid:
                    error_msg = f"{processing_date}: validation failed ({len(errors)} errors)"
                    self.logger.warning(f"Skipping {error_msg}")
                    self.error_messages.append(error_msg)
                    failed += 1
                    current_date += timedelta(days=1)
                    continue
                
                # Store reference metadata from first valid file
                if reference_metadata is None:
                    reference_metadata = metadata
                    self.logger.debug("Established reference metadata")
                
                # Step 4: Convert to xarray Dataset
                dataset = self.converter.tiff_to_dataset(file_path, processing_date)
                
                # Step 5: Append to Zarr store (with idempotency guard)
                self.converter.append_to_zarr(
                    dataset,
                    self.zarr_path,
                    processing_date,
                    allow_duplicate=False  # Enforce idempotency
                )
                
                successful += 1
                self.logger.info(
                    f"[OK] Successfully ingested {processing_date} "
                    f"({successful}/{len(downloaded_files)} processed)"
                )
                
            except (ValidationError, ZarrConversionError) as e:
                error_msg = f"{processing_date}: {str(e)}"
                self.logger.error(f"Failed to process {error_msg}")
                self.error_messages.append(error_msg)
                failed += 1
            except Exception as e:
                error_msg = f"{processing_date}: Unexpected error - {str(e)}"
                self.logger.error(
                    f"Unexpected error processing {processing_date}: {e}",
                    exc_info=True
                )
                self.error_messages.append(error_msg)
                failed += 1
            
            # Move to next day
            current_date += timedelta(days=1)
        
        processing_duration = time.time() - processing_start
        
        self.logger.info(
            f"Processing phase completed in {processing_duration:.2f}s: "
            f"{successful} successful, {failed} failed, {skipped} skipped"
        )
        
        return successful, failed, skipped
    
    def _update_final_metadata(self) -> None:
        """Update Zarr metadata after successful ingestion."""
        try:
            from datetime import datetime, timezone
            
            latest_date = self.state_manager.get_latest_date()
            
            updates = {
                'time_coverage_end': latest_date.isoformat(),
                'date_modified': datetime.now(timezone.utc).isoformat(),
                'last_incremental_update': datetime.now(timezone.utc).isoformat()
            }
            
            self.converter.update_metadata(self.zarr_path, updates)
            self.logger.info(f"Updated metadata: time_coverage_end={latest_date}")
            
        except Exception as e:
            self.logger.error(f"Failed to update metadata: {e}")
            # Don't raise - data is already written
    
    def _generate_summary(
        self,
        start_date: date,
        successful: int,
        failed: int,
        skipped: int,
        duration: float,
        last_download_date: Optional[date] = None,
        dry_run: bool = False
    ) -> Dict:
        """
        Generate comprehensive summary report.
        
        Args:
            start_date: First date attempted
            successful: Number of successfully ingested dates
            failed: Number of failed dates
            skipped: Number of skipped dates (already existed)
            duration: Total workflow duration in seconds
            last_download_date: Last date successfully downloaded
            dry_run: Whether this was a dry run
        
        Returns:
            Dictionary with summary information
        """
        # Get updated Zarr state
        latest_date = self.state_manager.get_latest_date()
        date_count = self.state_manager.get_date_count()
        coverage_stats = self.state_manager.get_coverage_stats()
        
        summary = {
            'mode': 'incremental',
            'dry_run': dry_run,
            'start_date': start_date.isoformat() if start_date else None,
            'last_download_date': last_download_date.isoformat() if last_download_date else None,
            'successful_ingestions': successful,
            'failed_ingestions': failed,
            'skipped_duplicates': skipped,
            'total_attempted': successful + failed + skipped,
            'duration_seconds': duration,
            'zarr_latest_date': latest_date.isoformat() if latest_date else None,
            'zarr_total_dates': date_count,
            'zarr_coverage_percent': coverage_stats.get('coverage_percent', 0.0),
            'zarr_has_gaps': coverage_stats.get('has_gaps', False),
            'next_expected_date': None
        }
        
        # Calculate next expected date
        if latest_date:
            next_expected = latest_date + timedelta(days=1)
            summary['next_expected_date'] = next_expected.isoformat()
        
        # Store summary for reporting
        self.summary = summary
        
        # Log summary
        self._print_summary(summary)
        
        # Audit log
        self.audit_logger.log_event("incremental_complete", summary)
        
        return summary
    
    def _print_summary(self, summary: Dict) -> None:
        """Print human-readable summary to logs."""
        self.logger.info("=" * 80)
        self.logger.info("INCREMENTAL INGESTION COMPLETE")
        self.logger.info("=" * 80)
        
        if summary['dry_run']:
            self.logger.info("MODE: DRY RUN (no changes made)")
        
        self.logger.info(f"Start date: {summary['start_date']}")
        self.logger.info(f"Last download: {summary['last_download_date']}")
        self.logger.info(f"Successfully ingested: {summary['successful_ingestions']}")
        self.logger.info(f"Failed: {summary['failed_ingestions']}")
        self.logger.info(f"Skipped (duplicates): {summary['skipped_duplicates']}")
        self.logger.info(f"Duration: {summary['duration_seconds']:.2f}s")
        self.logger.info("")
        self.logger.info(f"Zarr store updated:")
        self.logger.info(f"  Latest date: {summary['zarr_latest_date']}")
        self.logger.info(f"  Total dates: {summary['zarr_total_dates']}")
        self.logger.info(f"  Coverage: {summary['zarr_coverage_percent']:.2f}%")
        self.logger.info(f"  Next expected: {summary['next_expected_date']}")
        self.logger.info("=" * 80)
    
    def _send_email_notification(
        self,
        success: bool,
        start_time: datetime,
        end_time: datetime,
        dates_checked: int,
        new_files_found: int,
        files_ingested: int,
        files_failed: int,
        next_expected_date: Optional[str] = None
    ) -> None:
        """
        Send email notification with execution summary.
        
        Args:
            success: Whether the execution completed successfully
            start_time: Execution start timestamp
            end_time: Execution end timestamp
            dates_checked: Number of dates checked for new data
            new_files_found: Number of new files discovered
            files_ingested: Number of files successfully ingested
            files_failed: Number of files that failed
            next_expected_date: Next date expected for incremental update
        """
        try:
            email_sent = self.email_notifier.send_incremental_notification(
                success=success,
                start_time=start_time,
                end_time=end_time,
                dates_checked=dates_checked,
                new_files_found=new_files_found,
                files_ingested=files_ingested,
                files_failed=files_failed,
                gaps_detected=self.gaps_detected if self.gaps_detected else None,
                error_messages=self.error_messages if self.error_messages else None,
                next_expected_date=next_expected_date
            )
            
            if email_sent:
                self.logger.info("Email notification sent successfully")
            else:
                self.logger.info("Email notification skipped (disabled or not configured)")
                
        except Exception as e:
            self.logger.error(f"Failed to send email notification: {e}", exc_info=True)
            # Don't raise - notification failure shouldn't break the workflow
    
    def get_summary(self) -> Dict:
        """Get the summary from the last run."""
        return self.summary
