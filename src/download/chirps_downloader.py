"""
CHIRPS data downloader with retry logic and concurrent download support.

Provides robust downloading of CHIRPS v3.0 daily precipitation GeoTIFF files
with automatic retry, progress tracking, and error handling.
"""

import asyncio
import time
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import Config
from src.utils.logging import AuditLogger, setup_logger


class CHIRPSDownloader:
    """
    Download manager for CHIRPS v3.0 daily precipitation data.
    
    Features:
    - Concurrent downloads with configurable worker count
    - Automatic retry with exponential backoff
    - Progress tracking and logging
    - File validation (existence, size)
    - Support for resume/skip existing files
    """
    
    def __init__(
        self,
        config: Config,
        audit_logger: Optional[AuditLogger] = None,
        skip_existing: bool = True
    ):
        """
        Initialize the CHIRPS downloader.
        
        Args:
            config: Application configuration
            audit_logger: Optional audit logger for tracking downloads
            skip_existing: Whether to skip already-downloaded files
        """
        self.config = config
        self.logger = setup_logger(
            self.config.get_logger_name(__name__),
            log_dir=self.config.BASE_DIR / "logs"
        )
        self.audit_logger = audit_logger
        self.skip_existing = skip_existing
        
        # Create download session with retry configuration
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """
        Create HTTP session with retry logic.
        
        Returns:
            Configured requests Session
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.MAX_RETRIES,
            backoff_factor=1,  # 1s, 2s, 4s, etc.
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def download_single(
        self,
        download_date: date,
        output_path: Optional[Path] = None,
        task_id: Optional[str] = None
    ) -> Tuple[bool, Optional[Path], Optional[str]]:
        """
        Download a single CHIRPS GeoTIFF file.
        
        Args:
            download_date: Date to download
            output_path: Optional custom output path (default: config.RAW_DIR)
            task_id: Optional task identifier for tracking
            
        Returns:
            Tuple of (success, file_path, error_message)
        """
        date_str = download_date.strftime("%Y-%m-%d")
        url = self.config.get_chirps_url(
            download_date.year,
            download_date.month,
            download_date.day
        )
        
        # Determine output path
        if output_path is None:
            filename = f"chirps-v3.0.rnl.{download_date.strftime('%Y.%m.%d')}.tif"
            output_path = self.config.RAW_DIR / str(download_date.year) / filename
        
        # Create parent directory
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Skip if file exists and skip_existing is True
        if self.skip_existing and output_path.exists():
            file_size = output_path.stat().st_size
            if file_size > 0:
                self.logger.info(
                    f"Skipping {date_str}: file already exists ({file_size:,} bytes)"
                )
                return True, output_path, None
        
        # Log download start
        if self.audit_logger:
            self.audit_logger.log_download_start(date_str, url, task_id)
        
        self.logger.info(f"Downloading {date_str} from {url}")
        
        start_time = time.time()
        attempt = 0
        last_error = None
        
        while attempt < self.config.MAX_RETRIES:
            try:
                # Make the request
                response = self.session.get(
                    url,
                    timeout=self.config.TIMEOUT_SECONDS,
                    stream=True
                )
                response.raise_for_status()
                
                # Download with progress tracking
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=self.config.CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                
                duration = time.time() - start_time
                
                # Verify file was written
                if not output_path.exists() or output_path.stat().st_size == 0:
                    raise IOError(f"Downloaded file is empty or missing: {output_path}")
                
                actual_size = output_path.stat().st_size
                
                self.logger.info(
                    f"Downloaded {date_str}: {actual_size:,} bytes in {duration:.2f}s "
                    f"({actual_size / duration / 1024 / 1024:.2f} MB/s)"
                )
                
                # Log success
                if self.audit_logger:
                    self.audit_logger.log_download_complete(
                        date_str, url, str(output_path),
                        actual_size, duration, task_id
                    )
                
                return True, output_path, None
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    # File doesn't exist on server - don't retry
                    error_msg = f"File not found (404): {url}"
                    self.logger.warning(error_msg)
                    if self.audit_logger:
                        self.audit_logger.log_download_error(date_str, url, error_msg, task_id)
                    return False, None, error_msg
                else:
                    last_error = str(e)
                    attempt += 1
                    if attempt < self.config.MAX_RETRIES:
                        wait_time = self.config.RETRY_DELAY_SECONDS * attempt
                        self.logger.warning(
                            f"Download failed for {date_str} (attempt {attempt}): {e}. "
                            f"Retrying in {wait_time}s..."
                        )
                        time.sleep(wait_time)
            
            except (requests.exceptions.RequestException, IOError) as e:
                last_error = str(e)
                attempt += 1
                if attempt < self.config.MAX_RETRIES:
                    wait_time = self.config.RETRY_DELAY_SECONDS * attempt
                    self.logger.warning(
                        f"Download failed for {date_str} (attempt {attempt}): {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
        
        # All retries exhausted
        error_msg = f"Failed after {self.config.MAX_RETRIES} attempts: {last_error}"
        self.logger.error(f"Download failed for {date_str}: {error_msg}")
        
        if self.audit_logger:
            self.audit_logger.log_download_error(date_str, url, error_msg, task_id)
        
        return False, None, error_msg
    
    def download_date_range(
        self,
        start_date: date,
        end_date: date,
        max_workers: Optional[int] = None
    ) -> Tuple[List[Path], List[Tuple[date, str]]]:
        """
        Download CHIRPS files for a date range using concurrent workers.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            max_workers: Number of concurrent download workers (default: config value)
            
        Returns:
            Tuple of (successful_files, failed_downloads)
            failed_downloads is list of (date, error_message) tuples
        """
        if max_workers is None:
            max_workers = self.config.DOWNLOAD_CONCURRENCY
        
        # Generate list of dates to download
        dates_to_download = []
        current_date = start_date
        while current_date <= end_date:
            dates_to_download.append(current_date)
            current_date += timedelta(days=1)
        
        total_days = len(dates_to_download)
        self.logger.info(
            f"Downloading {total_days} files from {start_date} to {end_date} "
            f"using {max_workers} workers"
        )
        
        successful_files = []
        failed_downloads = []
        
        # Download concurrently using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all download tasks
            future_to_date = {
                executor.submit(
                    self.download_single,
                    download_date,
                    task_id=f"batch_{download_date.strftime('%Y%m%d')}"
                ): download_date
                for download_date in dates_to_download
            }
            
            # Process completed downloads
            completed = 0
            for future in as_completed(future_to_date):
                download_date = future_to_date[future]
                completed += 1
                
                try:
                    success, file_path, error_msg = future.result()
                    
                    if success and file_path:
                        successful_files.append(file_path)
                    else:
                        failed_downloads.append((download_date, error_msg or "Unknown error"))
                    
                    # Log progress
                    if completed % 10 == 0 or completed == total_days:
                        self.logger.info(
                            f"Progress: {completed}/{total_days} downloads attempted "
                            f"({len(successful_files)} successful, {len(failed_downloads)} failed)"
                        )
                
                except Exception as e:
                    self.logger.error(f"Unexpected error processing {download_date}: {e}")
                    failed_downloads.append((download_date, str(e)))
        
        self.logger.info(
            f"Download complete: {len(successful_files)} successful, "
            f"{len(failed_downloads)} failed out of {total_days} total"
        )
        
        return successful_files, failed_downloads
    
    def verify_download(self, file_path: Path) -> bool:
        """
        Verify a downloaded file exists and has content.
        
        Args:
            file_path: Path to the file to verify
            
        Returns:
            True if file is valid, False otherwise
        """
        if not file_path.exists():
            self.logger.warning(f"File does not exist: {file_path}")
            return False
        
        size = file_path.stat().st_size
        if size == 0:
            self.logger.warning(f"File is empty: {file_path}")
            return False
        
        self.logger.debug(f"Verified file: {file_path} ({size:,} bytes)")
        return True
    
    def cleanup_session(self) -> None:
        """Close the HTTP session and release resources."""
        if self.session:
            self.session.close()
            self.logger.debug("HTTP session closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup_session()
