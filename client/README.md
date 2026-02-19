# CHIRPS Zarr Client

A comprehensive client library for accessing and testing the CHIRPS v3.0 Zarr store with support for various access patterns, spatial/temporal subsetting, and concurrent operations.

## Features

- **Multiple Access Patterns**: Single date, date ranges, spatial subsets, named regions
- **Temporal Aggregation**: Compute means, sums, and other statistics over time
- **Spatial Subsetting**: Extract data for specific regions or custom bounding boxes
- **Concurrent Access**: Multi-threaded and async access patterns for performance
- **Performance Benchmarking**: Built-in tools to test different access patterns
- **Data Validation**: Validate data quality and check for anomalies

## Files

- `config.py` - Client configuration (paths, regions, date ranges, performance tuning)
- `zarr_client.py` - Main client class with all access methods
- `examples.py` - Comprehensive examples demonstrating all features
- `examples2.py` - **Simple data explorer for extracting precipitation data**
- `README.md` - This file

## Quick Start

### Simple Data Extraction (examples2.py)

For quick data extraction without complex operations, use the `ChirpsDataExplorer`:

```python
from examples2 import ChirpsDataExplorer

# Extract data for a single point
with ChirpsDataExplorer() as explorer:
    df = explorer.get_precipitation_data(
        latitude=42.0,
        longitude=-93.5,
        start_date="2025-01-01",
        end_date="2025-01-31",
        method='nearest'  # or 'interp' for interpolation
    )
    
    # df is a pandas DataFrame with columns: date, precipitation_mm
    print(df)
    
    # Save to CSV
    df.to_csv("output.csv", index=False)
```

**To customize**: Edit the `custom_query()` function in `examples2.py` and run:
```bash
python examples2.py
```

### Advanced Client Usage (zarr_client.py)

For more complex operations, use the full `ChirpsZarrClient`:

```python
from zarr_client import ChirpsZarrClient

# Context manager automatically handles open/close
with ChirpsZarrClient() as client:
    # Get metadata
    metadata = client.get_metadata()
    print(metadata)
    
    # Access single date
    data = client.get_single_date("2024-01-01")
    print(data['precipitation'].mean())
    
    # Access date range
    data = client.get_date_range("2024-01-01", "2024-01-31")
    print(f"Shape: {data['precipitation'].shape}")
```

### Spatial Subsetting

```python
with ChirpsZarrClient() as client:
    # Named region
    data = client.get_region(
        "east_africa",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    
    # Custom bounding box
    data = client.get_spatial_subset(
        lon_min=35.0, lon_max=50.0,
        lat_min=-5.0, lat_max=15.0,
        start_date="2024-01-01",
        end_date="2024-12-31"
    )
```

### Temporal Aggregation

```python
with ChirpsZarrClient() as client:
    # Monthly mean
    mean = client.compute_temporal_mean(
        "2024-01-01", "2024-01-31",
        region="east_africa"
    )
    
    # Seasonal accumulation
    total = client.compute_temporal_sum(
        "2024-03-01", "2024-05-31",
        region="sahel"
    )
```

### Parallel Access

```python
from datetime import datetime, timedelta

# Generate date list
base = datetime(2024, 1, 1)
dates = [(base + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(10)]

# Parallel access with thread pool
client = ChirpsZarrClient()
results = client.parallel_date_access(dates, max_workers=4)
print(f"Retrieved {len(results)} datasets")
client.close()
```

### Async Access

```python
import asyncio

async def fetch_data():
    dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
    
    client = ChirpsZarrClient()
    results = await client.async_date_access(dates)
    client.close()
    
    return results

results = asyncio.run(fetch_data())
```

### Multi-Region Analysis

```python
with ChirpsZarrClient() as client:
    regions = ["east_africa", "west_africa", "southern_africa"]
    
    stats = client.parallel_region_analysis(
        regions,
        start_date="2024-01-01",
        end_date="2024-01-31",
        max_workers=4
    )
    
    for region, data in stats.items():
        print(f"{region}: {data['mean']:.2f} mm/day")
```

### Performance Benchmarking

```python
with ChirpsZarrClient() as client:
    # Benchmark single date access
    stats = client.benchmark_access(
        'single_date',
        n_iterations=10,
        date="2024-01-15"
    )
    print(f"Mean: {stats['mean']:.3f}s, Std: {stats['std']:.3f}s")
    
    # Benchmark regional subset
    stats = client.benchmark_access(
        'region',
        n_iterations=5,
        region="east_africa",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    print(f"Mean: {stats['mean']:.3f}s")
```

### Data Validation

```python
with ChirpsZarrClient() as client:
    # Validate a specific date
    results = client.validate_data("2024-01-01")
    
    print(f"Has data: {results['has_data']}")
    print(f"No negatives: {results['no_negatives']}")
    print(f"Valid pixels: {results['valid_percent']:.1f}%")
    print(f"Mean: {results['mean_value']:.2f} mm/day")
    print(f"Status: {'PASSED' if results['all_passed'] else 'FAILED'}")
```

## Configuration

Edit `config.py` to customize:

### Test Regions
```python
test_regions = {
    "east_africa": (25.0, 50.0, -15.0, 15.0),
    "west_africa": (-20.0, 20.0, 0.0, 20.0),
    "southern_africa": (10.0, 40.0, -35.0, -15.0),
    "sahel": (-20.0, 50.0, 10.0, 20.0),
    "horn_of_africa": (35.0, 50.0, -5.0, 15.0),
    "global": (-180.0, 180.0, -50.0, 50.0),
}
```

### Performance Tuning
```python
chunk_cache_size = 256 * 1024 * 1024  # 256 MB
max_workers = 4  # Thread pool size

dask_chunks = {
    "time": 30,
    "latitude": 500,
    "longitude": 500,
}
```

## Running Examples

Run all example scenarios:

```bash
cd client
python examples.py
```

This will demonstrate:
1. Basic data access patterns
2. Spatial subsetting for regions
3. Temporal aggregation operations
4. Parallel data access
5. Async data access
6. Multi-region analysis
7. Performance benchmarking
8. Data validation

## Environment Variables

Optional environment variables for configuration:

- `ZARR_CHUNK_CACHE_MB` - Chunk cache size in MB (default: 256)
- `CLIENT_MAX_WORKERS` - Maximum worker threads (default: 4)

## Performance Tips

1. **Use chunking wisely**: The default chunks (30, 500, 500) balance memory and I/O
2. **Parallel access**: Use thread pools for independent date/region queries
3. **Spatial subsetting**: Extract only the region you need before computing
4. **Lazy evaluation**: Use `.compute()` only when you need actual values
5. **Persistent connections**: Use context manager or single client instance for multiple queries

## Common Use Cases

### Time Series Analysis
```python
with ChirpsZarrClient() as client:
    # Extract time series for a location
    point_data = client.get_spatial_subset(
        lon_min=36.8, lon_max=36.9,
        lat_min=-1.3, lat_max=-1.2,
        start_date="2024-01-01",
        end_date="2024-12-31"
    )
    time_series = point_data['precipitation'].mean(dim=['latitude', 'longitude'])
```

### Climatology Computation
```python
with ChirpsZarrClient() as client:
    # Compute annual climatology for a region
    annual_mean = client.compute_temporal_mean(
        "2023-01-01", "2024-12-31",
        region="east_africa"
    )
```

### Anomaly Detection
```python
with ChirpsZarrClient() as client:
    # Get long-term mean
    climatology = client.compute_temporal_mean(
        "2023-01-01", "2024-12-31",
        region="sahel"
    )
    
    # Get specific period
    current = client.compute_temporal_mean(
        "2024-06-01", "2024-08-31",
        region="sahel"
    )
    
    # Compute anomaly
    anomaly = current - climatology
```

## Troubleshooting

**FileNotFoundError**: Ensure the Zarr store exists at the configured path and bootstrap ingestion has completed.

**Memory issues**: Reduce chunk sizes in config or use smaller spatial/temporal subsets.

**Slow performance**: 
- Increase `max_workers` for parallel operations
- Use spatial subsetting to reduce data volume
- Check chunk cache size setting

## API Reference

See docstrings in `zarr_client.py` for detailed API documentation.

## License

Same as parent project.
