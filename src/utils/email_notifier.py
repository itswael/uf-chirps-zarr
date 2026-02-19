"""
Email Notification Module for CHIRPS Data Pipeline

This module provides email notification functionality for bootstrap and incremental
ingestion processes, sending formatted execution summaries to configured recipients.
"""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

from src.config import get_config

logger = logging.getLogger(__name__)


class EmailNotifier:
    """
    Handles email notifications for CHIRPS data pipeline execution summaries.
    
    Sends formatted HTML emails with execution details including:
    - Execution type (bootstrap/incremental)
    - Start and end times
    - Date ranges processed
    - Success/failure counts
    - Error messages
    - Performance metrics
    """
    
    def __init__(self):
        """Initialize the email notifier with configuration."""
        self.config = get_config()
        self._recipients: Optional[List[str]] = None
    
    def is_enabled(self) -> bool:
        """
        Check if email notifications are enabled.
        
        Returns:
            True if email notifications are properly configured and enabled.
        """
        if not self.config.EMAIL_ENABLED:
            return False
        
        if not self.config.EMAIL_FROM:
            logger.warning("Email notifications enabled but EMAIL_FROM not configured")
            return False
        
        if not self.config.SMTP_USERNAME:
            logger.warning("Email notifications enabled but SMTP_USERNAME not configured")
            return False
        
        if not self.config.SMTP_PASSWORD:
            logger.warning("Email notifications enabled but SMTP_PASSWORD not configured")
            return False
        
        return True
    
    def get_recipients(self) -> List[str]:
        """
        Load email recipients from configuration file.
        
        Returns:
            List of email addresses.
        """
        if self._recipients is not None:
            return self._recipients
        
        recipients_file = self.config.EMAIL_RECIPIENTS_FILE
        if not recipients_file.exists():
            logger.warning(f"Recipients file not found: {recipients_file}")
            return []
        
        try:
            with open(recipients_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Parse emails, skip comments and empty lines
            recipients = []
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#'):
                    recipients.append(line)
            
            self._recipients = recipients
            return recipients
            
        except Exception as e:
            logger.error(f"Failed to read recipients file: {e}")
            return []
    
    def send_bootstrap_notification(
        self,
        success: bool,
        start_time: datetime,
        end_time: datetime,
        date_range: tuple,
        files_processed: int,
        files_failed: int,
        zarr_store_size_mb: Optional[float] = None,
        error_messages: Optional[List[str]] = None
    ) -> bool:
        """
        Send email notification for bootstrap ingestion execution.
        
        Args:
            success: Whether the execution completed successfully
            start_time: Execution start timestamp
            end_time: Execution end timestamp
            date_range: Tuple of (start_date, end_date) processed
            files_processed: Number of files successfully processed
            files_failed: Number of files that failed
            zarr_store_size_mb: Final Zarr store size in MB
            error_messages: List of error messages encountered
        
        Returns:
            True if email sent successfully, False otherwise.
        """
        if not self.is_enabled():
            logger.info("Email notifications disabled, skipping bootstrap notification")
            return False
        
        recipients = self.get_recipients()
        if not recipients:
            logger.warning("No email recipients configured")
            return False
        
        duration = end_time - start_time
        status = "SUCCESS" if success else "FAILURE"
        status_color = "#28a745" if success else "#dc3545"
        
        subject = f"CHIRPS Bootstrap Ingestion {status} - {date_range[0]} to {date_range[1]}"
        
        html_body = self._generate_bootstrap_html(
            status=status,
            status_color=status_color,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            date_range=date_range,
            files_processed=files_processed,
            files_failed=files_failed,
            zarr_store_size_mb=zarr_store_size_mb,
            error_messages=error_messages or []
        )
        
        return self._send_email(subject, html_body, recipients)
    
    def send_incremental_notification(
        self,
        success: bool,
        start_time: datetime,
        end_time: datetime,
        dates_checked: int,
        new_files_found: int,
        files_ingested: int,
        files_failed: int,
        gaps_detected: Optional[List[str]] = None,
        error_messages: Optional[List[str]] = None,
        next_expected_date: Optional[str] = None
    ) -> bool:
        """
        Send email notification for incremental ingestion execution.
        
        Args:
            success: Whether the execution completed successfully
            start_time: Execution start timestamp
            end_time: Execution end timestamp
            dates_checked: Number of dates checked for new data
            new_files_found: Number of new files discovered
            files_ingested: Number of files successfully ingested
            files_failed: Number of files that failed
            gaps_detected: List of date gaps found in the data
            error_messages: List of error messages encountered
            next_expected_date: Next date expected for incremental update
        
        Returns:
            True if email sent successfully, False otherwise.
        """
        if not self.is_enabled():
            logger.info("Email notifications disabled, skipping incremental notification")
            return False
        
        recipients = self.get_recipients()
        if not recipients:
            logger.warning("No email recipients configured")
            return False
        
        duration = end_time - start_time
        status = "SUCCESS" if success else "FAILURE"
        status_color = "#28a745" if success else "#dc3545"
        
        subject = f"CHIRPS Incremental Ingestion {status} - {new_files_found} new files"
        
        html_body = self._generate_incremental_html(
            status=status,
            status_color=status_color,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            dates_checked=dates_checked,
            new_files_found=new_files_found,
            files_ingested=files_ingested,
            files_failed=files_failed,
            gaps_detected=gaps_detected or [],
            error_messages=error_messages or [],
            next_expected_date=next_expected_date
        )
        
        return self._send_email(subject, html_body, recipients)
    
    def _generate_bootstrap_html(
        self,
        status: str,
        status_color: str,
        start_time: datetime,
        end_time: datetime,
        duration: timedelta,
        date_range: tuple,
        files_processed: int,
        files_failed: int,
        zarr_store_size_mb: Optional[float],
        error_messages: List[str]
    ) -> str:
        """Generate HTML email body for bootstrap notification."""
        
        duration_str = str(duration).split('.')[0]  # Remove microseconds
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background-color: {status_color}; color: white; padding: 20px; text-align: center; border-radius: 5px 5px 0 0; }}
        .content {{ background-color: #f9f9f9; padding: 20px; border: 1px solid #ddd; border-top: none; }}
        .metric {{ margin: 10px 0; padding: 10px; background-color: white; border-left: 4px solid #007bff; }}
        .metric-label {{ font-weight: bold; color: #555; }}
        .metric-value {{ color: #007bff; font-size: 1.1em; }}
        .section {{ margin: 20px 0; }}
        .section-title {{ font-size: 1.2em; font-weight: bold; color: #333; border-bottom: 2px solid #007bff; padding-bottom: 5px; margin-bottom: 10px; }}
        .error {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 10px; margin: 5px 0; }}
        .footer {{ text-align: center; margin-top: 20px; padding: 10px; color: #777; font-size: 0.9em; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>CHIRPS Bootstrap Ingestion Report</h1>
            <h2 style="margin: 10px 0;">Status: {status}</h2>
        </div>
        
        <div class="content">
            <div class="section">
                <div class="section-title">Execution Details</div>
                <div class="metric">
                    <span class="metric-label">Start Time:</span>
                    <span class="metric-value">{start_time.strftime('%Y-%m-%d %H:%M:%S')}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">End Time:</span>
                    <span class="metric-value">{end_time.strftime('%Y-%m-%d %H:%M:%S')}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Duration:</span>
                    <span class="metric-value">{duration_str}</span>
                </div>
            </div>
            
            <div class="section">
                <div class="section-title">Data Processing Summary</div>
                <div class="metric">
                    <span class="metric-label">Date Range:</span>
                    <span class="metric-value">{date_range[0]} to {date_range[1]}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Files Processed:</span>
                    <span class="metric-value">{files_processed}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Files Failed:</span>
                    <span class="metric-value">{files_failed}</span>
                </div>
                {f'<div class="metric"><span class="metric-label">Zarr Store Size:</span><span class="metric-value">{zarr_store_size_mb:.2f} MB</span></div>' if zarr_store_size_mb else ''}
            </div>
"""
        
        if error_messages:
            html += """
            <div class="section">
                <div class="section-title">Errors Encountered</div>
"""
            for error in error_messages[:10]:  # Limit to first 10 errors
                html += f'                <div class="error">{error}</div>\n'
            
            if len(error_messages) > 10:
                html += f'                <div class="error">... and {len(error_messages) - 10} more errors</div>\n'
            
            html += "            </div>\n"
        
        html += """
        </div>
        
        <div class="footer">
            <p>This is an automated notification from the CHIRPS Data Pipeline.</p>
            <p>Generated at """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
        </div>
    </div>
</body>
</html>
"""
        
        return html
    
    def _generate_incremental_html(
        self,
        status: str,
        status_color: str,
        start_time: datetime,
        end_time: datetime,
        duration: timedelta,
        dates_checked: int,
        new_files_found: int,
        files_ingested: int,
        files_failed: int,
        gaps_detected: List[str],
        error_messages: List[str],
        next_expected_date: Optional[str]
    ) -> str:
        """Generate HTML email body for incremental notification."""
        
        duration_str = str(duration).split('.')[0]  # Remove microseconds
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background-color: {status_color}; color: white; padding: 20px; text-align: center; border-radius: 5px 5px 0 0; }}
        .content {{ background-color: #f9f9f9; padding: 20px; border: 1px solid #ddd; border-top: none; }}
        .metric {{ margin: 10px 0; padding: 10px; background-color: white; border-left: 4px solid #007bff; }}
        .metric-label {{ font-weight: bold; color: #555; }}
        .metric-value {{ color: #007bff; font-size: 1.1em; }}
        .section {{ margin: 20px 0; }}
        .section-title {{ font-size: 1.2em; font-weight: bold; color: #333; border-bottom: 2px solid #007bff; padding-bottom: 5px; margin-bottom: 10px; }}
        .warning {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 10px; margin: 5px 0; }}
        .error {{ background-color: #f8d7da; border-left: 4px solid #dc3545; padding: 10px; margin: 5px 0; }}
        .footer {{ text-align: center; margin-top: 20px; padding: 10px; color: #777; font-size: 0.9em; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>CHIRPS Incremental Ingestion Report</h1>
            <h2 style="margin: 10px 0;">Status: {status}</h2>
        </div>
        
        <div class="content">
            <div class="section">
                <div class="section-title">Execution Details</div>
                <div class="metric">
                    <span class="metric-label">Start Time:</span>
                    <span class="metric-value">{start_time.strftime('%Y-%m-%d %H:%M:%S')}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">End Time:</span>
                    <span class="metric-value">{end_time.strftime('%Y-%m-%d %H:%M:%S')}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Duration:</span>
                    <span class="metric-value">{duration_str}</span>
                </div>
            </div>
            
            <div class="section">
                <div class="section-title">Data Processing Summary</div>
                <div class="metric">
                    <span class="metric-label">Dates Checked:</span>
                    <span class="metric-value">{dates_checked}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">New Files Found:</span>
                    <span class="metric-value">{new_files_found}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Files Ingested:</span>
                    <span class="metric-value">{files_ingested}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Files Failed:</span>
                    <span class="metric-value">{files_failed}</span>
                </div>
                {f'<div class="metric"><span class="metric-label">Next Expected Date:</span><span class="metric-value">{next_expected_date}</span></div>' if next_expected_date else ''}
            </div>
"""
        
        if gaps_detected:
            html += """
            <div class="section">
                <div class="section-title">Data Gaps Detected</div>
"""
            for gap in gaps_detected[:10]:  # Limit to first 10 gaps
                html += f'                <div class="warning">Missing data for: {gap}</div>\n'
            
            if len(gaps_detected) > 10:
                html += f'                <div class="warning">... and {len(gaps_detected) - 10} more gaps</div>\n'
            
            html += "            </div>\n"
        
        if error_messages:
            html += """
            <div class="section">
                <div class="section-title">Errors Encountered</div>
"""
            for error in error_messages[:10]:  # Limit to first 10 errors
                html += f'                <div class="error">{error}</div>\n'
            
            if len(error_messages) > 10:
                html += f'                <div class="error">... and {len(error_messages) - 10} more errors</div>\n'
            
            html += "            </div>\n"
        
        html += """
        </div>
        
        <div class="footer">
            <p>This is an automated notification from the CHIRPS Data Pipeline.</p>
            <p>Generated at """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
        </div>
    </div>
</body>
</html>
"""
        
        return html
    
    def _send_email(self, subject: str, html_body: str, recipients: List[str]) -> bool:
        """
        Send HTML email to recipients.
        
        Args:
            subject: Email subject line
            html_body: HTML content for email body
            recipients: List of recipient email addresses
        
        Returns:
            True if email sent successfully, False otherwise.
        """
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.config.EMAIL_FROM
            msg['To'] = ', '.join(recipients)
            
            # Attach HTML body
            html_part = MIMEText(html_body, 'html')
            msg.attach(html_part)
            
            # Connect to SMTP server and send
            logger.info(f"Connecting to SMTP server {self.config.SMTP_HOST}:{self.config.SMTP_PORT}")
            
            with smtplib.SMTP(self.config.SMTP_HOST, self.config.SMTP_PORT) as server:
                if self.config.SMTP_USE_TLS:
                    server.starttls()
                
                server.login(self.config.SMTP_USERNAME, self.config.SMTP_PASSWORD)
                server.send_message(msg)
            
            logger.info(f"Email notification sent successfully to {len(recipients)} recipients")
            return True
            
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP authentication failed: {e}")
            return False
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error occurred: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False
