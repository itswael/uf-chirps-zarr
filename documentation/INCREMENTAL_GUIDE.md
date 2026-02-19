# Incremental Ingestion Guide

## Overview

The incremental ingestion system allows you to automatically update your CHIRPS Zarr store with newly available data. It is designed to handle the uncertainty in CHIRPS data release schedules (typically previous month's data becomes available between days 11-27 of the current month).

## How It Works

### Data Availability Pattern

CHIRPS provides daily precipitation data with the following schedule:
- **Previous month's data** (31 daily files) becomes available sometime during the current month
- **Upload date varies** between day 11-27 of the month (no fixed schedule)
- **Solution**: Check daily for new data availability

### Incremental Workflow

The incremental ingestion system follows this workflow (per TDD Section 8):

1. **Determine Next Expected Date**
   - Read the latest date from existing Zarr store
   - Calculate next expected date (latest + 1 day)

2. **Check and Download**
   - Attempt to download consecutive days starting from next expected date
   - Stop when file not available (404 response)
   - Support up to 31 days per run (one month)

3. **Validate and Process** (for each day)
   - Validate file integrity (readable, non-empty)
   - Validate CRS (must be EPSG:4326)
   - Validate spatial grid consistency
   - Check for duplicates (idempotency guard)
   - Convert GeoTIFF to xarray Dataset
   - Append to Zarr store

4. **Update Metadata**
   - Update time_coverage_end
   - Record last modification date
   - Log completion

5. **Generate Summary Report**
   - Number of days ingested
   - Failed/skipped dates
   - Updated Zarr store statistics
   - Next expected date

## Usage

### Command-Line Interface

#### Auto Mode (Recommended)

Automatically selects bootstrap or incremental based on Zarr store existence:

```bash
# Auto-select mode
python -m src.cli auto

# Auto-mode with dry-run (check without downloading)
python -m src.cli auto --dry-run

# Auto-mode without confirmation prompt
python -m src.cli auto -y
```

#### Explicit Incremental Mode

```bash
# Basic incremental update
python -m src.cli incremental

# Incremental with options
python -m src.cli incremental --max-days 31 --dry-run

# Force start from specific date (overrides automatic detection)
python -m src.cli incremental --force-date 2024-02-01

# Skip confirmation prompt
python -m src.cli incremental -y
```

#### Check System Status

```bash
# View comprehensive system status
python -m src.cli status

# View detailed Zarr store information
python -m src.cli info
```

### Programmatic Usage

```python
from datetime import date
from src.config import get_config
from src.orchestration.incremental_ingestion import IncrementalOrchestrator

# Initialize orchestrator
config = get_config()
orchestrator = IncrementalOrchestrator(
    config=config,
    max_days_per_run=31
)

# Run incremental ingestion
summary = orchestrator.run()

# Check results
print(f"Ingested {summary['successful_ingestions']} days")
print(f"Next expected: {summary['next_expected_date']}")

# Access detailed summary
if summary['failed_ingestions'] > 0:
    print(f"Failed: {summary['failed_ingestions']} days")
```

### Dry-Run Mode

Test availability without downloading or modifying the Zarr store:

```python
orchestrator = IncrementalOrchestrator(config=config)
summary = orchestrator.run(dry_run=True)
```

## Key Features

### 1. Automatic Mode Selection

The system automatically determines whether to run bootstrap or incremental:

```python
from src.utils.zarr_state import ZarrStateManager

state_manager = ZarrStateManager(zarr_path)

if state_manager.exists():
    # Run incremental
    print("Zarr exists - running incremental update")
else:
    # Run bootstrap
    print("Zarr doesn't exist - running bootstrap")
```

### 2. Idempotency Guards

The system prevents duplicate date ingestion:

```python
# Check before appending
if converter.check_date_exists(zarr_path, date(2024, 2, 1)):
    print("Date already exists - skipping")
else:
    converter.append_to_zarr(dataset, zarr_path, date(2024, 2, 1))
```

Idempotency ensures:
- ✅ Safe to re-run incremental ingestion
- ✅ No data corruption from duplicates
- ✅ Clear logging of skipped dates

### 3. Consecutive Day Detection

Downloads continue until data is unavailable:

```python
# Downloads Feb 1, 2, 3, ... until 404 or max days
files, failures, last_date = downloader.download_incremental(
    start_date=date(2024, 2, 1),
    max_consecutive_days=31,
    stop_on_missing=True
)

print(f"Downloaded up to {last_date}")
```

### 4. Comprehensive State Tracking

Monitor Zarr store state:

```python
from src.utils.zarr_state import ZarrStateManager

manager = ZarrStateManager(zarr_path)

# Get statistics
stats = manager.get_coverage_stats()
print(f"Latest date: {stats['latest_date']}")
print(f"Total days: {stats['total_dates']}")
print(f"Coverage: {stats['coverage_percent']}%")
print(f"Has gaps: {stats['has_gaps']}")

# Find missing dates
missing = manager.find_missing_dates()
print(f"Missing dates: {missing}")

# Get next expected date
next_date = manager.get_next_expected_date()
print(f"Next expected: {next_date}")
```

### 5. Gap Detection and Filling

The system can detect and fill gaps:

```python
# Find gaps in the sequence
missing_dates = manager.find_missing_dates(
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 31)
)

if missing_dates:
    print(f"Found {len(missing_dates)} gaps")
    
    # Fill gaps by running incremental with force-date
    for gap_date in missing_dates:
        orchestrator.run(force_date=gap_date)
```

## Scheduling Recommendations

### Daily Cron Job

Since CHIRPS data availability is uncertain (day 11-27), run daily:

```bash
# crontab entry - run at 2 AM daily
0 2 * * * cd /path/to/uf-chirps-zarr && python -m src.cli auto -y >> logs/cron.log 2>&1
```

The `auto` command will:
- Check if new data is available
- Download and ingest if available
- Do nothing if data not yet released
- Be idempotent (safe to run even if already up-to-date)

### Monitoring Script

```python
#!/usr/bin/env python
"""Daily monitoring script for CHIRPS ingestion."""

from datetime import date, timedelta
from src.utils.zarr_state import get_zarr_state_manager

def check_update_needed():
    """Check if Zarr store needs updating."""
    manager = get_zarr_state_manager()
    
    if not manager.exists():
        return "BOOTSTRAP_NEEDED"
    
    latest = manager.get_latest_date()
    today = date.today()
    days_behind = (today - latest).days
    
    if days_behind > 31:
        return f"CRITICAL: {days_behind} days behind"
    elif days_behind > 7:
        return f"WARNING: {days_behind} days behind"
    else:
        return f"OK: {days_behind} days behind"

if __name__ == '__main__':
    print(check_update_needed())
```

## Error Handling

### Failed Downloads

The system distinguishes between types of failures:

```
# File not available (404) - data not yet uploaded
INFO: Incremental: File not available for 2024-02-15. Data not yet uploaded.
Stopping incremental download.

# Network error - real failure
WARNING: Incremental: Failed to download 2024-02-15: Connection timeout
```

### Validation Failures

Files that fail validation are skipped:

```
WARNING: Skipping 2024-02-15: validation failed (3 errors)
  - Unexpected CRS: EPSG:4269, expected EPSG:4326
  - Width mismatch: 7200 vs 3600
  - Unrealistic max precipitation: 3000.0 mm/day
```

### Idempotency Violations

Attempting to ingest duplicate dates:

```
INFO: Skipping 2024-02-15: already exists in Zarr (idempotency)
```

### Recovery

All failures leave the Zarr store unchanged:
- Failed ingestions don't modify the store
- Temporary files are cleaned up
- Safe to re-run after fixing issues

## Summary Reports

After each run, the system generates a comprehensive summary:

```
================================================================================
INCREMENTAL INGESTION COMPLETE
================================================================================
Start date: 2024-02-01
Last download: 2024-02-15
Successfully ingested: 15 days
Failed: 0 days
Skipped (duplicates): 0 days
Duration: 347.23s

Zarr store updated:
  Latest date: 2024-02-15
  Total dates: 776 days
  Coverage: 99.87%
  Next expected: 2024-02-16
================================================================================
```

The summary includes:
- Ingestion statistics (successful/failed/skipped)
- Updated Zarr state
- Performance metrics
- Next action needed

## Best Practices

### 1. Run Daily via Cron

```bash
# Run automatically every day
python -m src.cli auto -y
```

### 2. Monitor Logs

```bash
# Check recent ingestion logs
tail -f logs/incremental.log

# Search for errors
grep ERROR logs/*.log
```

### 3. Use Dry-Run to Test

```bash
# Test before running actual ingestion
python -m src.cli incremental --dry-run
```

### 4. Check Status Regularly

```bash
# Weekly status check
python -m src.cli status
```

### 5. Handle Gaps Proactively

```python
# Weekly gap check
manager = get_zarr_state_manager()
missing = manager.find_missing_dates()

if missing:
    send_alert(f"Found {len(missing)} missing dates")
```

## Troubleshooting

### "Zarr store does not exist"

**Problem**: Running incremental before bootstrap

**Solution**:
```bash
# Run bootstrap first
python -m src.cli bootstrap --start-date 2023-01-01 --end-date 2024-12-31

# Or use auto mode (will automatically select bootstrap)
python -m src.cli auto
```

### "Date already exists in Zarr"

**Problem**: Attempting to re-ingest date

**Solution**: This is normal - the idempotency guard is working. The date will be skipped automatically.

### No New Data Available

**Problem**: Running incremental when CHIRPS hasn't uploaded new data yet

**Solution**: This is expected. The system will log "File not available" and exit cleanly. Run again tomorrow.

### Performance Issues

**Problem**: Incremental ingestion is slow

**Solution**: Incremental processes one file at a time by design (simplicity over throughput). For batch processing, consider the performance optimizations documented in the performance analysis.

## Architecture

### Components

1. **ZarrStateManager** (`src/utils/zarr_state.py`)
   - Tracks Zarr store state
   - Detects latest date, missing dates, gaps
   - Provides coverage statistics

2. **IncrementalOrchestrator** (`src/orchestration/incremental_ingestion.py`)
   - Coordinates the workflow
   - Manages download → validate → convert → append pipeline
   - Generates summary reports

3. **CHIRPSDownloader.download_incremental()** (`src/download/chirps_downloader.py`)
   - Downloads consecutive days
   - Stops on 404 (data not available)

4. **TIFFToZarrConverter** (`src/convert/tiff_to_zarr.py`)
   - Idempotency checks via `check_date_exists()`
   - Chunk-safe appends
   - Metadata updates

### Design Principles

1. **Simplicity Over Throughput**
   - One file at a time
   - Clear, sequential workflow
   - Easy to debug and monitor

2. **Idempotency**
   - Safe to re-run
   - No duplicates
   - Predictable behavior

3. **Correctness Over Speed**
   - Validate thoroughly
   - Fail safely
   - Maintain data integrity

4. **Clear Failure Handling**
   - Detailed error messages
   - Comprehensive logging
   - Rollback safety

## See Also

- [Bootstrap Ingestion Guide](./bootstrap_guide.md)
- [Configuration Guide](./configuration.md)
- [Performance Optimization](./performance.md)
- [API Reference](./api_reference.md)
