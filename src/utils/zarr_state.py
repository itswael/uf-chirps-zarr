"""
Zarr store state management utilities.

Provides functions to:
- Read the latest ingested date from a Zarr store
- Detect missing dates in temporal sequence
- Check if a date already exists in the store
- Track ingestion state for incremental updates

Addresses GitHub Issue #18: Missing-date detector
"""

from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional, Set, Tuple

import pandas as pd
import xarray as xr

from src.config import Config
from src.utils.logging import setup_logger


class ZarrStateError(Exception):
    """Exception raised during Zarr state operations."""
    pass


class ZarrStateManager:
    """
    Manager for tracking the state of a CHIRPS Zarr store.
    
    Provides utilities to:
    - Determine if Zarr store exists and is valid
    - Get latest ingested date
    - Check if specific dates exist
    - Detect gaps in temporal sequence
    - Calculate next expected date for incremental ingestion
    """
    
    def __init__(self, zarr_path: Path, config: Optional[Config] = None):
        """
        Initialize the Zarr state manager.
        
        Args:
            zarr_path: Path to the Zarr store
            config: Optional application configuration
        """
        self.zarr_path = zarr_path
        self.config = config
        self.logger = setup_logger(
            "chirps_zarr.utils.zarr_state",
            log_dir=zarr_path.parent.parent.parent / "logs"
        )
        
        # Cache for performance
        self._existing_dates_cache: Optional[Set[date]] = None
        self._cache_valid = False
    
    def exists(self) -> bool:
        """
        Check if the Zarr store exists.
        
        Returns:
            True if Zarr store exists and is readable, False otherwise
        """
        if not self.zarr_path.exists():
            return False
        
        try:
            # Try to open and validate it's a valid Zarr store
            ds = xr.open_zarr(self.zarr_path)
            has_time = 'time' in ds.dims
            has_precip = 'precipitation' in ds.data_vars
            ds.close()
            return has_time and has_precip
        except Exception as e:
            self.logger.warning(f"Zarr store exists but cannot be opened: {e}")
            return False
    
    def is_bootstrap_complete(self) -> bool:
        """
        Check if bootstrap ingestion was completed.
        
        Returns:
            True if bootstrap_complete flag is set in metadata
        """
        if not self.exists():
            return False
        
        try:
            ds = xr.open_zarr(self.zarr_path)
            is_complete = ds.attrs.get('bootstrap_complete', False)
            ds.close()
            return is_complete
        except Exception as e:
            self.logger.error(f"Error checking bootstrap status: {e}")
            return False
    
    def get_latest_date(self) -> Optional[date]:
        """
        Get the latest (most recent) date in the Zarr store.
        
        Returns:
            Latest date, or None if store is empty or doesn't exist
        """
        if not self.exists():
            return None
        
        try:
            ds = xr.open_zarr(self.zarr_path)
            
            if len(ds.time) == 0:
                ds.close()
                return None
            
            # Get the last time value
            latest_time = pd.to_datetime(ds.time.values[-1])
            latest_date = latest_time.date()
            
            ds.close()
            
            self.logger.debug(f"Latest date in Zarr: {latest_date}")
            return latest_date
            
        except Exception as e:
            raise ZarrStateError(f"Failed to get latest date from Zarr: {e}")
    
    def get_earliest_date(self) -> Optional[date]:
        """
        Get the earliest (first) date in the Zarr store.
        
        Returns:
            Earliest date, or None if store is empty or doesn't exist
        """
        if not self.exists():
            return None
        
        try:
            ds = xr.open_zarr(self.zarr_path)
            
            if len(ds.time) == 0:
                ds.close()
                return None
            
            # Get the first time value
            earliest_time = pd.to_datetime(ds.time.values[0])
            earliest_date = earliest_time.date()
            
            ds.close()
            
            self.logger.debug(f"Earliest date in Zarr: {earliest_date}")
            return earliest_date
            
        except Exception as e:
            raise ZarrStateError(f"Failed to get earliest date from Zarr: {e}")
    
    def get_date_count(self) -> int:
        """
        Get the total number of dates in the Zarr store.
        
        Returns:
            Number of time steps in the store
        """
        if not self.exists():
            return 0
        
        try:
            ds = xr.open_zarr(self.zarr_path)
            count = len(ds.time)
            ds.close()
            return count
        except Exception as e:
            raise ZarrStateError(f"Failed to get date count from Zarr: {e}")
    
    def get_existing_dates(self, use_cache: bool = True) -> Set[date]:
        """
        Get all dates currently in the Zarr store.
        
        Args:
            use_cache: Whether to use cached results (default: True)
        
        Returns:
            Set of dates present in the store
        """
        # Return cached results if available and valid
        if use_cache and self._cache_valid and self._existing_dates_cache is not None:
            return self._existing_dates_cache
        
        if not self.exists():
            return set()
        
        try:
            ds = xr.open_zarr(self.zarr_path)
            
            # Convert time values to dates
            time_values = pd.to_datetime(ds.time.values)
            existing_dates = set(time_values.date)
            
            ds.close()
            
            # Update cache
            self._existing_dates_cache = existing_dates
            self._cache_valid = True
            
            self.logger.debug(f"Found {len(existing_dates)} dates in Zarr")
            return existing_dates
            
        except Exception as e:
            raise ZarrStateError(f"Failed to get existing dates from Zarr: {e}")
    
    def invalidate_cache(self) -> None:
        """Invalidate the cached existing dates."""
        self._cache_valid = False
        self._existing_dates_cache = None
    
    def date_exists(self, check_date: date) -> bool:
        """
        Check if a specific date exists in the Zarr store.
        
        Args:
            check_date: Date to check
        
        Returns:
            True if date exists in store, False otherwise
        """
        existing_dates = self.get_existing_dates(use_cache=True)
        return check_date in existing_dates
    
    def get_next_expected_date(self) -> Optional[date]:
        """
        Calculate the next expected date for incremental ingestion.
        
        This is latest_date + 1 day.
        
        Returns:
            Next expected date, or None if store doesn't exist
        """
        latest = self.get_latest_date()
        if latest is None:
            return None
        
        next_date = latest + timedelta(days=1)
        self.logger.debug(f"Next expected date: {next_date}")
        return next_date
    
    def find_missing_dates(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[date]:
        """
        Find missing dates in a date range.
        
        Identifies gaps in the daily sequence where data should exist
        but is missing from the Zarr store.
        
        Args:
            start_date: Start of range to check (default: earliest date in Zarr)
            end_date: End of range to check (default: latest date in Zarr)
        
        Returns:
            List of missing dates in ascending order
        """
        if not self.exists():
            return []
        
        # Use Zarr bounds if not specified
        if start_date is None:
            start_date = self.get_earliest_date()
        if end_date is None:
            end_date = self.get_latest_date()
        
        if start_date is None or end_date is None:
            return []
        
        # Get existing dates
        existing_dates = self.get_existing_dates(use_cache=True)
        
        # Generate expected date range
        expected_dates = []
        current = start_date
        while current <= end_date:
            expected_dates.append(current)
            current += timedelta(days=1)
        
        # Find missing dates
        missing_dates = [d for d in expected_dates if d not in existing_dates]
        
        if missing_dates:
            self.logger.info(
                f"Found {len(missing_dates)} missing dates between "
                f"{start_date} and {end_date}"
            )
        
        return sorted(missing_dates)
    
    def has_gaps(self) -> bool:
        """
        Check if there are any gaps in the temporal sequence.
        
        Returns:
            True if gaps exist, False otherwise
        """
        missing = self.find_missing_dates()
        return len(missing) > 0
    
    def get_date_range(self) -> Optional[Tuple[date, date]]:
        """
        Get the complete date range covered by the Zarr store.
        
        Returns:
            Tuple of (earliest_date, latest_date), or None if store is empty
        """
        earliest = self.get_earliest_date()
        latest = self.get_latest_date()
        
        if earliest is None or latest is None:
            return None
        
        return (earliest, latest)
    
    def get_coverage_stats(self) -> dict:
        """
        Get comprehensive coverage statistics for the Zarr store.
        
        Returns:
            Dictionary with coverage statistics including:
            - exists: Whether store exists
            - bootstrap_complete: Bootstrap completion status
            - earliest_date: First date in store
            - latest_date: Last date in store
            - total_dates: Number of dates in store
            - expected_dates: Number of dates that should exist in range
            - missing_dates: Number of missing dates
            - coverage_percent: Percentage of expected dates present
            - has_gaps: Whether there are gaps in the sequence
        """
        stats = {
            'exists': self.exists(),
            'bootstrap_complete': self.is_bootstrap_complete(),
            'earliest_date': None,
            'latest_date': None,
            'total_dates': 0,
            'expected_dates': 0,
            'missing_dates': 0,
            'coverage_percent': 0.0,
            'has_gaps': False
        }
        
        if not stats['exists']:
            return stats
        
        try:
            earliest = self.get_earliest_date()
            latest = self.get_latest_date()
            
            stats['earliest_date'] = earliest
            stats['latest_date'] = latest
            stats['total_dates'] = self.get_date_count()
            
            if earliest and latest:
                # Calculate expected dates in range
                days_diff = (latest - earliest).days + 1
                stats['expected_dates'] = days_diff
                
                # Find missing dates
                missing = self.find_missing_dates(earliest, latest)
                stats['missing_dates'] = len(missing)
                stats['has_gaps'] = len(missing) > 0
                
                # Calculate coverage percentage
                if days_diff > 0:
                    stats['coverage_percent'] = (
                        (stats['total_dates'] / days_diff) * 100
                    )
            
        except Exception as e:
            self.logger.error(f"Error getting coverage stats: {e}")
        
        return stats
    
    def print_summary(self) -> None:
        """Print a human-readable summary of the Zarr store state."""
        stats = self.get_coverage_stats()
        
        print("\n" + "="*80)
        print("ZARR STORE STATE SUMMARY")
        print("="*80)
        print(f"Path: {self.zarr_path}")
        print(f"Exists: {stats['exists']}")
        
        if not stats['exists']:
            print("Store does not exist or is invalid.")
            print("="*80 + "\n")
            return
        
        print(f"Bootstrap Complete: {stats['bootstrap_complete']}")
        print(f"\nTemporal Coverage:")
        print(f"  Earliest Date: {stats['earliest_date']}")
        print(f"  Latest Date: {stats['latest_date']}")
        print(f"  Total Days: {stats['total_dates']}")
        
        if stats['expected_dates'] > 0:
            print(f"\nData Completeness:")
            print(f"  Expected Days: {stats['expected_dates']}")
            print(f"  Missing Days: {stats['missing_dates']}")
            print(f"  Coverage: {stats['coverage_percent']:.2f}%")
            print(f"  Has Gaps: {stats['has_gaps']}")
        
        if stats['latest_date']:
            next_date = stats['latest_date'] + timedelta(days=1)
            print(f"\nNext Expected Date: {next_date}")
        
        print("="*80 + "\n")


def get_zarr_state_manager(
    zarr_path: Optional[Path] = None,
    config: Optional[Config] = None
) -> ZarrStateManager:
    """
    Factory function to create a ZarrStateManager.
    
    Args:
        zarr_path: Path to Zarr store (uses config default if None)
        config: Configuration object
    
    Returns:
        Configured ZarrStateManager instance
    """
    if config is None:
        from src.config import get_config
        config = get_config()
    
    if zarr_path is None:
        zarr_path = config.ZARR_STORE_PATH
    
    return ZarrStateManager(zarr_path, config)
