"""
Logging utilities for the CHIRPS Zarr climate data platform.

Provides standardized logging configuration with support for:
- Console and file logging
- JSON-structured audit logs
- Contextual logging with request/task IDs
- Performance tracking
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from logging.handlers import RotatingFileHandler


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    
    Outputs log records as JSON objects with timestamp, level, message,
    and additional context fields.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record as JSON.
        
        Args:
            record: The log record to format
            
        Returns:
            JSON-formatted log string
        """
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add any extra fields from the record
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)
        
        return json.dumps(log_data)


class AuditLogger:
    """
    Audit logger for tracking critical operations.
    
    Records ingestion events, data validation results, and system state
    changes to a separate audit log file.
    """
    
    def __init__(self, log_dir: Path, name: str = "audit"):
        """
        Initialize the audit logger.
        
        Args:
            log_dir: Directory for audit log files
            name: Logger name
        """
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(f"chirps_zarr.audit.{name}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        
        # Remove existing handlers
        self.logger.handlers.clear()
        
        # Add file handler with JSON formatting
        audit_file = self.log_dir / f"audit_{name}.log"
        file_handler = RotatingFileHandler(
            audit_file,
            maxBytes=100 * 1024 * 1024,  # 100MB
            backupCount=10
        )
        file_handler.setFormatter(JSONFormatter())
        self.logger.addHandler(file_handler)
    
    def log_download_start(self, date: str, url: str, task_id: Optional[str] = None) -> None:
        """
        Log the start of a download operation.
        
        Args:
            date: Date being downloaded (YYYY-MM-DD format)
            url: Download URL
            task_id: Optional task identifier
        """
        extra_fields = {
            "event": "download_start",
            "date": date,
            "url": url,
            "task_id": task_id
        }
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, "", 0, "Download started", (),
            None, extra={"extra_fields": extra_fields}
        )
        self.logger.handle(record)
    
    def log_download_complete(
        self,
        date: str,
        url: str,
        file_path: str,
        size_bytes: int,
        duration_seconds: float,
        task_id: Optional[str] = None
    ) -> None:
        """
        Log successful download completion.
        
        Args:
            date: Date downloaded
            url: Source URL
            file_path: Local file path
            size_bytes: File size in bytes
            duration_seconds: Download duration
            task_id: Optional task identifier
        """
        extra_fields = {
            "event": "download_complete",
            "date": date,
            "url": url,
            "file_path": file_path,
            "size_bytes": size_bytes,
            "duration_seconds": round(duration_seconds, 3),
            "task_id": task_id
        }
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, "", 0, "Download completed", (),
            None, extra={"extra_fields": extra_fields}
        )
        self.logger.handle(record)
    
    def log_download_error(
        self,
        date: str,
        url: str,
        error: str,
        task_id: Optional[str] = None
    ) -> None:
        """
        Log download failure.
        
        Args:
            date: Date that failed
            url: Source URL
            error: Error message
            task_id: Optional task identifier
        """
        extra_fields = {
            "event": "download_error",
            "date": date,
            "url": url,
            "error": error,
            "task_id": task_id
        }
        record = self.logger.makeRecord(
            self.logger.name, logging.ERROR, "", 0, "Download failed", (),
            None, extra={"extra_fields": extra_fields}
        )
        self.logger.handle(record)
    
    def log_validation_result(
        self,
        date: str,
        file_path: str,
        is_valid: bool,
        errors: Optional[list] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log raster validation result.
        
        Args:
            date: Date of the raster
            file_path: Path to validated file
            is_valid: Whether validation passed
            errors: List of validation errors (if any)
            metadata: Optional raster metadata
        """
        extra_fields = {
            "event": "validation_complete",
            "date": date,
            "file_path": file_path,
            "is_valid": is_valid,
            "errors": errors or [],
            "metadata": metadata or {}
        }
        level = logging.INFO if is_valid else logging.WARNING
        message = "Validation passed" if is_valid else "Validation failed"
        record = self.logger.makeRecord(
            self.logger.name, level, "", 0, message, (),
            None, extra={"extra_fields": extra_fields}
        )
        self.logger.handle(record)
    
    def log_zarr_append(
        self,
        date: str,
        zarr_path: str,
        time_index: int,
        duration_seconds: float
    ) -> None:
        """
        Log successful Zarr append operation.
        
        Args:
            date: Date appended
            zarr_path: Path to Zarr store
            time_index: Time index where data was appended
            duration_seconds: Append operation duration
        """
        extra_fields = {
            "event": "zarr_append",
            "date": date,
            "zarr_path": zarr_path,
            "time_index": time_index,
            "duration_seconds": round(duration_seconds, 3)
        }
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, "", 0, "Zarr append completed", (),
            None, extra={"extra_fields": extra_fields}
        )
        self.logger.handle(record)
    
    def log_zarr_init(
        self,
        zarr_path: str,
        dimensions: Dict[str, int],
        chunks: Dict[str, int],
        start_date: str,
        metadata: Dict[str, Any]
    ) -> None:
        """
        Log Zarr store initialization.
        
        Args:
            zarr_path: Path to new Zarr store
            dimensions: Dimension sizes
            chunks: Chunk sizes
            start_date: Initial time coverage start
            metadata: Dataset metadata
        """
        extra_fields = {
            "event": "zarr_init",
            "zarr_path": zarr_path,
            "dimensions": dimensions,
            "chunks": chunks,
            "start_date": start_date,
            "metadata": metadata
        }
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, "", 0, "Zarr store initialized", (),
            None, extra={"extra_fields": extra_fields}
        )
        self.logger.handle(record)
    
    def log_bootstrap_start(
        self,
        start_date: str,
        end_date: str,
        total_days: int
    ) -> None:
        """
        Log bootstrap ingestion start.
        
        Args:
            start_date: Bootstrap start date
            end_date: Bootstrap end date
            total_days: Total days to process
        """
        extra_fields = {
            "event": "bootstrap_start",
            "start_date": start_date,
            "end_date": end_date,
            "total_days": total_days
        }
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, "", 0, "Bootstrap ingestion started", (),
            None, extra={"extra_fields": extra_fields}
        )
        self.logger.handle(record)
    
    def log_bootstrap_complete(
        self,
        start_date: str,
        end_date: str,
        total_days: int,
        successful: int,
        failed: int,
        duration_seconds: float
    ) -> None:
        """
        Log bootstrap ingestion completion.
        
        Args:
            start_date: Bootstrap start date
            end_date: Bootstrap end date
            total_days: Total days processed
            successful: Number of successful ingestions
            failed: Number of failed ingestions
            duration_seconds: Total duration
        """
        extra_fields = {
            "event": "bootstrap_complete",
            "start_date": start_date,
            "end_date": end_date,
            "total_days": total_days,
            "successful": successful,
            "failed": failed,
            "duration_seconds": round(duration_seconds, 3)
        }
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, "", 0, "Bootstrap ingestion completed", (),
            None, extra={"extra_fields": extra_fields}
        )
        self.logger.handle(record)
    
    def log_event(
        self,
        event_type: str,
        metadata: Dict[str, Any],
        level: int = logging.INFO,
        message: Optional[str] = None
    ) -> None:
        """
        Log a generic event with arbitrary metadata.
        
        This is a flexible method for logging events that don't fit
        into the predefined logging methods.
        
        Args:
            event_type: Type of event (e.g., "incremental_start", "gap_detected")
            metadata: Dictionary of event-specific metadata
            level: Logging level (default: INFO)
            message: Optional human-readable message (default: capitalized event_type)
        """
        if message is None:
            message = event_type.replace("_", " ").capitalize()
        
        extra_fields = {"event": event_type}
        extra_fields.update(metadata)
        
        record = self.logger.makeRecord(
            self.logger.name, level, "", 0, message, (),
            None, extra={"extra_fields": extra_fields}
        )
        self.logger.handle(record)


def setup_logger(
    name: str,
    log_dir: Optional[Path] = None,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
    json_format: bool = False
) -> logging.Logger:
    """
    Set up a standardized logger for the application.
    
    Args:
        name: Logger name (usually module __name__)
        log_dir: Directory for log files (None for console-only)
        console_level: Logging level for console output
        file_level: Logging level for file output
        json_format: Whether to use JSON formatting for files
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Capture all, filter at handler level
    logger.propagate = False
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Console handler with simple formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler if log directory is provided
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{name.split('.')[-1]}.log"
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=50 * 1024 * 1024,  # 50MB
            backupCount=5
        )
        file_handler.setLevel(file_level)
        
        if json_format:
            file_handler.setFormatter(JSONFormatter())
        else:
            file_format = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_format)
        
        logger.addHandler(file_handler)
    
    return logger
