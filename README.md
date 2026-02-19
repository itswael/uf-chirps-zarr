# CHIRPS Zarr Climate Data Platform

A production-grade climate data platform for ingesting CHIRPS precipitation raster TIFF files, preprocessing them, converting them to Zarr format, and supporting efficient querying at TB-scale.

## Features

- **Bootstrap Ingestion**: Initial historical backfill of CHIRPS data
- **Incremental Updates**: Automatic daily/monthly updates with idempotency
- **Automatic Mode Selection**: Intelligently choose bootstrap or incremental
- **Data Validation**: Comprehensive spatial, temporal, and format validation
- **Zarr Conversion**: Convert TIFF files to efficient Zarr format with chunking
- **State Tracking**: Monitor coverage, detect gaps, track latest ingested date
- **Query Support**: Fast spatial and temporal queries on multi-year datasets
- **Scalable**: Handles TB-level data volumes with Dask

## Quick Start

### Installation

```bash
# Clone repository
git clone <repository-url>
cd uf-chirps-zarr

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Usage

#### Auto Mode (Recommended)

Automatically selects bootstrap or incremental based on store existence:

```bash
# First run - will perform bootstrap
python -m src.cli auto

# Subsequent runs - will perform incremental updates
python -m src.cli auto
```

#### Bootstrap (Initial Setup)

```bash
# Bootstrap with default dates (from config)
python -m src.cli bootstrap

# Bootstrap with custom date range
python -m src.cli bootstrap --start-date 2023-01-01 --end-date 2024-12-31
```

#### Incremental Updates

```bash
# Check for and ingest new data
python -m src.cli incremental

# Dry-run (check availability without downloading)
python -m src.cli incremental --dry-run

# Force start from specific date
python -m src.cli incremental --force-date 2024-02-01
```

#### Monitor Status

```bash
# Comprehensive system status
python -m src.cli status

# Detailed Zarr store information  
python -m src.cli info
```

## Requirements

- Python 3.11+
- See `requirements.txt` for dependencies

## Project Structure

```
src/
├── config.py                           # Centralized configuration
├── cli.py                              # Command-line interface
├── download/
│   └── chirps_downloader.py           # CHIRPS data downloader with incremental support
├── preprocess/
│   └── raster_cleaner.py              # Raster validation
├── convert/
│   └── tiff_to_zarr.py                # TIFF to Zarr conversion with idempotency
├── orchestration/
│   ├── bootstrap_ingestion.py         # Bootstrap workflow orchestrator
│   └── incremental_ingestion.py       # Incremental workflow orchestrator
└── utils/
    ├── logging.py                      # Logging utilities
    └── zarr_state.py                   # Zarr store state management

data/
├── raw/                               # Downloaded TIFF files
├── interim/                           # Preprocessed data  
└── zarr/                              # Zarr stores

config/
└── metadata.json                      # Zarr metadata configuration

documentation/
├── INCREMENTAL_GUIDE.md               # Comprehensive incremental guide
└── BootstrapSystemArchitecture.drawio # System architecture diagrams

tests/                                 # Test suite
├── test_config.py                     # Configuration tests
└── test_incremental.py                # Incremental integration tests

logs/                                  # Application and audit logs
```

## Ingestion Modes

### Bootstrap Mode

Initial one-time historical backfill:
- Downloads all historical CHIRPS data for specified date range
- Initializes Zarr store with proper metadata
- Validates and processes files sequentially
- Typically used once at system setup

### Incremental Mode

Daily/monthly updates for ongoing operations:
- Detects next expected date from existing Zarr store
- Downloads consecutive newly available days
- Idempotent (safe to re-run, prevents duplicates)
- Stops when data not yet available
- Updates metadata with latest coverage

### Auto Mode

Intelligent mode selection:
- If Zarr store doesn't exist → runs bootstrap
- If Zarr store exists → runs incremental
- No manual intervention needed
- Recommended for automated scheduling

## Configuration

All configuration is centralized in `src/config.py`. Settings can be overridden via environment variables:

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CHIRPS_BASE_DIR` | Project root | Base directory |
| `CHIRPS_RAW_DIR` | `data/raw` | Raw TIFF storage |
| `CHIRPS_INTERIM_DIR` | `data/interim` | Preprocessed data |
| `CHIRPS_ZARR_DIR` | `data/zarr` | Zarr stores |
| `CHIRPS_DOWNLOAD_CONCURRENCY` | `4` | Concurrent downloads |
| `CHIRPS_CHUNK_SIZE` | `8388608` | Download chunk size (bytes) |
| `CHIRPS_BASE_URL` | CHIRPS official | Data source URL |
| `CHIRPS_TIMEOUT_SECONDS` | `300` | HTTP timeout |
| `CHIRPS_MAX_RETRIES` | `3` | Max retry attempts |
| `CHIRPS_RETRY_DELAY_SECONDS` | `5` | Retry delay |
| `CHIRPS_BOOTSTRAP_START_DATE` | `2020-01-01` | Initial ingest start |
| `CHIRPS_BOOTSTRAP_END_DATE` | `2020-12-31` | Initial ingest end |
| `CHIRPS_METADATA_CONFIG` | `config/metadata.json` | Metadata config path |

### Metadata Configuration

Customize Zarr metadata by editing `config/metadata.json`:

```json
{
  "title": "CHIRPS Daily Precipitation Data",
  "institution": "Climate Hazards Center, UC Santa Barbara",
  "source": "CHIRPS version 2.0",
  "variable_attributes": {
    "precipitation": {
      "long_name": "Daily precipitation",
      "units": "mm/day"
    }
  }
}
```

## Scheduling for Production

### Daily Cron Job (Recommended)

Since CHIRPS data availability is uncertain (typically day 11-27 of each month), schedule incremental ingestion to run daily:

```bash
# crontab entry - run at 2 AM daily
0 2 * * * cd /path/to/uf-chirps-zarr && /path/to/.venv/bin/python -m src.cli auto -y >> logs/cron.log 2>&1
```

The `auto` command will:
- ✅ Detect new data availability
- ✅ Download and ingest if available  
- ✅ Do nothing if data not yet released
- ✅ Be idempotent (safe to run repeatedly)

### Monitoring

```bash
# Check status regularly
python -m src.cli status

# View logs
tail -f logs/incremental.log

# Check for errors
grep ERROR logs/*.log
```

## Programmatic Usage

### Bootstrap Example

```python
from datetime import date
from src.config import get_config
from src.orchestration.bootstrap_ingestion import BootstrapOrchestrator

# Get configuration
config = get_config()

# Create orchestrator
orchestrator = BootstrapOrchestrator(
    config=config,
    start_date=date(2023, 1, 1),
    end_date=date(2024, 12, 31)
)

# Run bootstrap ingestion
total, successful, failed = orchestrator.run()
print(f"Ingested {successful}/{total} days")
```

### Incremental Example

```python
from src.orchestration.incremental_ingestion import IncrementalOrchestrator

# Create orchestrator
orchestrator = IncrementalOrchestrator(max_days_per_run=31)

# Run incremental update
summary = orchestrator.run()

# Check results
print(f"Ingested: {summary['successful_ingestions']} days")
print(f"Next expected: {summary['next_expected_date']}")
```

### State Tracking Example

```python
from src.utils.zarr_state import get_zarr_state_manager

# Get state manager
manager = get_zarr_state_manager()

# Get statistics
stats = manager.get_coverage_stats()
print(f"Latest: {stats['latest_date']}")
print(f"Coverage: {stats['coverage_percent']}%")
print(f"Total days: {stats['total_dates']}")

# Find gaps
missing = manager.find_missing_dates()
if missing:
    print(f"Found {len(missing)} gaps: {missing}")
```

## Documentation

- **[Incremental Ingestion Guide](documentation/INCREMENTAL_GUIDE.md)** - Comprehensive guide for incremental updates
- **[System Architecture](documentation/BootstrapSystemArchitecture.drawio)** - Architecture diagrams
- **[Implementation Summary](IMPLEMENTATION_SUMMARY.md)** - Technical implementation details

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src

# Run specific test file
pytest tests/test_config.py -v
pytest tests/test_incremental.py -v
```

## Key Design Principles

### 1. Idempotency
- Safe to re-run ingestion
- Automatic duplicate detection
- No data corruption from re-runs

### 2. Automatic Mode Selection
- Bootstrap if Zarr doesn't exist
- Incremental if Zarr exists
- No manual configuration needed

### 3. Comprehensive Validation
- Spatial consistency checks
- Temporal sequence validation
- Format and integrity verification

### 4. State Tracking
- Monitor coverage and gaps
- Track latest ingested date
- Calculate next expected date

### 5. Clear Error Handling
- Detailed error messages
- Comprehensive logging
- Safe failure recovery

## Troubleshooting

### Common Issues

**"Zarr store does not exist"**
```bash
# Solution: Run bootstrap first
python -m src.cli bootstrap
# Or use auto mode
python -m src.cli auto
```

**"Date already exists in Zarr"**
- This is normal - idempotency guard working
- Date will be automatically skipped

**"File not available (404)"**
- CHIRPS data not yet uploaded
- Run again tomorrow
- Normal between day 11-27 of month

**Slow performance**
- See performance analysis in docs
- Incremental designed for simplicity over speed
- Consider batch optimizations for large backfills

## Development Guidelines

- **Python 3.11+**: Target version
- **Type hints**: Required for all functions
- **Docstrings**: Google style, comprehensive
- **No globals**: Use config singleton pattern
- **Error handling**: Comprehensive try/except with specific exceptions
- **Logging**: Log all meaningful operations at appropriate levels
- **Testing**: Write tests for all modules, aim for >80% coverage
- **Git commits**: Meaningful commit messages with GitHub issue references

## License

[Add your license here]

## Contributors

[Add contributors here]
