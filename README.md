# CHIRPS Zarr Climate Data Platform

A production-grade climate data platform for ingesting CHIRPS precipitation raster TIFF files, preprocessing them, converting them to Zarr format, and supporting efficient querying at TB-scale.

## Features

- **Data Ingestion**: Download CHIRPS daily precipitation data
- **Preprocessing**: Clean and validate raster data
- **Zarr Conversion**: Convert TIFF files to efficient Zarr format
- **Query Support**: Fast spatial and temporal queries on multi-year datasets
- **Scalable**: Handles TB-level data volumes with Dask

## Requirements

- Python 3.11+
- See `requirements.txt` for dependencies

## Project Structure

```
src/
├── config.py                    # Centralized configuration
├── cli.py                       # Command-line interface
├── download/
│   └── chirps_downloader.py    # CHIRPS data downloader
├── preprocess/
│   └── raster_cleaner.py       # Raster validation
├── convert/
│   └── tiff_to_zarr.py         # TIFF to Zarr conversion
├── orchestration/
│   └── bootstrap_ingestion.py  # Bootstrap workflow orchestrator
└── utils/
    └── logging.py              # Logging utilities

data/
├── raw/                        # Downloaded TIFF files
├── interim/                    # Preprocessed data  
└── zarr/                       # Zarr stores

config/
└── metadata.json               # Zarr metadata configuration

tests/                          # Test suite
logs/                           # Application and audit logs
```

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

## Installation

```bash
# Clone repository
git clone <repo-url>
cd uf-chirps-zarr

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Bootstrap Ingestion

Run the bootstrap ingestion to create the initial Zarr store with historical data:

```bash
# View help
python -m src.cli bootstrap --help

# Run bootstrap with default dates (from config)
python -m src.cli bootstrap

# Run bootstrap with custom date range
python -m src.cli bootstrap --start-date 2024-01-01 --end-date 2024-12-31

# Run without confirmation prompt
python -m src.cli bootstrap -y

# Skip download (use existing files)
python -m src.cli bootstrap --skip-download
```

### View Zarr Store Info

```bash
python -m src.cli info
```

### View Configuration

```bash
python -m src.cli config
```

### Programmatic Usage

```python
from datetime import date
from src.config import get_config
from src.orchestration.bootstrap_ingestion import BootstrapOrchestrator

# Get configuration
config = get_config()

# Create orchestrator
orchestrator = BootstrapOrchestrator(
    config=config,
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31)
)

# Run bootstrap ingestion
total, successful, failed = orchestrator.run()

print(f"Ingested {successful}/{total} days")
```

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src

# Run specific test file
pytest tests/test_config.py -v
```

## Development Guidelines

- **Python 3.11**: Target version
- **Type hints**: Required for all functions
- **Docstrings**: Google style
- **No globals**: Use config singleton
- **Error handling**: Comprehensive try/except
- **Logging**: Log all meaningful operations
- **Testing**: Write tests for all modules

## License

[Add your license here]

## Contributors

[Add contributors here]
