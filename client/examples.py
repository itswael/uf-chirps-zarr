"""
Example usage scenarios for CHIRPS Zarr client.

This script demonstrates various access patterns and use cases
for the CHIRPS Zarr client.
"""
import asyncio
from datetime import datetime, timedelta
from pprint import pprint

from config import config
from zarr_client import ChirpsZarrClient


def example_1_basic_access():
    """Example 1: Basic data access patterns."""
    print("\n" + "="*80)
    print("EXAMPLE 1: Basic Data Access")
    print("="*80)
    
    with ChirpsZarrClient() as client:
        # Get metadata
        print("\n1. Dataset Metadata:")
        metadata = client.get_metadata()
        pprint(metadata)
        
        # Single date access
        print("\n2. Single Date Access (2024-03-01):")
        data = client.get_single_date("2024-03-01")
        print(f"   Shape: {data['precipitation'].shape}")
        print(f"   Mean precipitation: {float(data['precipitation'].mean())} mm/day")
        
        # Date range access
        print("start time:", datetime.now())
        print("\n3. Date Range Access (2023-01-01 to 2024-01-07):")
        data = client.get_date_range("2023-01-01", "2024-01-07")
        print(f"   Shape: {data['precipitation'].shape}")
        print(f"   Time steps: {len(data.time)}")
        print("end time:", datetime.now())


def example_2_spatial_subsetting():
    """Example 2: Spatial subsetting for regions."""
    print("\n" + "="*80)
    print("EXAMPLE 2: Spatial Subsetting")
    print("="*80)
    
    with ChirpsZarrClient() as client:
        # Get East Africa region
        print("\n1. East Africa Region (Jan 2024):")
        data = client.get_region("angola", "2024-01-01", "2024-01-31")
        precip = data['precipitation']
        
        # Filter out fill values
        valid_precip = precip.values[precip.values != -9999.0]
        
        print(f"   Spatial extent: {data.longitude.min().values:.2f}°E to {data.longitude.max().values:.2f}°E")
        print(f"   Latitude range: {data.latitude.min().values:.2f}°N to {data.latitude.max().values:.2f}°N")
        print(f"   Time steps: {len(data.time)}")
        print(f"   Mean precipitation: {valid_precip.mean():.2f} mm/day")
        print(f"   Max precipitation: {valid_precip.max():.2f} mm/day")
        
        # Custom spatial subset
        print("\n2. Custom Spatial Subset (Horn of Africa):")
        data = client.get_spatial_subset(
            lon_min=35.0, lon_max=50.0,
            lat_min=-5.0, lat_max=15.0,
            start_date="2024-06-01",
            end_date="2024-06-30"
        )
        print(f"   Data shape: {data['precipitation'].shape}")


def example_3_temporal_aggregation():
    """Example 3: Temporal aggregation operations."""
    print("\n" + "="*80)
    print("EXAMPLE 3: Temporal Aggregation")
    print("="*80)
    
    with ChirpsZarrClient() as client:
        # Monthly mean
        print("\n1. January 2024 Mean Precipitation (West Africa):")
        monthly_mean = client.compute_temporal_mean(
            "2024-01-01", "2024-01-31", region="west_africa"
        )
        valid_mean = monthly_mean.values[monthly_mean.values != -9999.0]
        print(f"   Mean: {valid_mean.mean():.2f} mm/day")
        print(f"   Std: {valid_mean.std():.2f} mm/day")
        
        # Seasonal accumulation
        print("\n2. Seasonal Accumulation (MAM 2024, West Africa):")
        seasonal_sum = client.compute_temporal_sum(
            "2024-03-01", "2024-05-31", region="west_africa"
        )
        valid_sum = seasonal_sum.values[seasonal_sum.values != -9999.0]
        print(f"   Total: {valid_sum.mean():.2f} mm")
        print(f"   Max: {valid_sum.max():.2f} mm")


def example_4_parallel_access():
    """Example 4: Parallel/concurrent data access."""
    print("\n" + "="*80)
    print("EXAMPLE 4: Parallel Data Access")
    print("="*80)
    
    # Generate list of dates
    base_date = datetime(2024, 1, 1)
    dates = [(base_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(10)]
    
    with ChirpsZarrClient() as client:
        # Sequential access
        print("\n1. Sequential Access (10 dates):")
        import time
        start = time.time()
        for date in dates:
            _ = client.get_single_date(date)
        sequential_time = time.time() - start
        print(f"   Time: {sequential_time:.2f}s")
        
    # Parallel access
    print("\n2. Parallel Access (10 dates, 4 workers):")
    start = time.time()
    client = ChirpsZarrClient()
    results = client.parallel_date_access(dates, max_workers=4)
    parallel_time = time.time() - start
    print(f"   Time: {parallel_time:.2f}s")
    print(f"   Speedup: {sequential_time/parallel_time:.2f}x")
    print(f"   Retrieved {len(results)} datasets")
    client.close()


def example_5_async_access():
    """Example 5: Async data access."""
    print("\n" + "="*80)
    print("EXAMPLE 5: Async Data Access")
    print("="*80)
    
    async def async_example():
        base_date = datetime(2024, 1, 1)
        dates = [(base_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(5)]
        
        client = ChirpsZarrClient()
        
        print(f"\nFetching {len(dates)} dates asynchronously...")
        import time
        start = time.time()
        results = await client.async_date_access(dates)
        elapsed = time.time() - start
        
        print(f"   Time: {elapsed:.2f}s")
        print(f"   Retrieved {len(results)} datasets")
        
        client.close()
    
    asyncio.run(async_example())


def example_6_multi_region_analysis():
    """Example 6: Parallel multi-region analysis."""
    print("\n" + "="*80)
    print("EXAMPLE 6: Multi-Region Analysis")
    print("="*80)
    
    regions = ["southern_africa", "angola", "west_africa", "sahel", "horn_of_africa", "wyoming"]
    
    with ChirpsZarrClient() as client:
        print(f"\nAnalyzing {len(regions)} regions for March 2024...")
        import time
        start = time.time()
        
        results = client.parallel_region_analysis(
            regions,
            start_date="2024-01-01",
            end_date="2024-01-31",
            max_workers=4
        )
        
        elapsed = time.time() - start
        print(f"   Time: {elapsed:.2f}s\n")
        
        for region, stats in results.items():
            # Adjust for fill values in mean
            print(f"{region}:")
            print(f"   Mean: {stats['mean']:.2f} mm/day")
            print(f"   Std:  {stats['std']:.2f} mm/day")
            print(f"   Max:  {stats['max']:.2f} mm/day")


def example_7_performance_benchmark():
    """Example 7: Performance benchmarking."""
    print("\n" + "="*80)
    print("EXAMPLE 7: Performance Benchmarking")
    print("="*80)
    
    with ChirpsZarrClient() as client:
        # Single date access
        print("\n1. Single Date Access (10 iterations):")
        stats = client.benchmark_access(
            'single_date',
            n_iterations=10,
            date="2024-01-15"
        )
        print(f"   Mean: {stats['mean']:.3f}s")
        print(f"   Std:  {stats['std']:.3f}s")
        
        # Date range access
        print("\n2. One Week Access (10 iterations):")
        stats = client.benchmark_access(
            'date_range',
            n_iterations=10,
            start_date="2024-01-01",
            end_date="2024-01-07"
        )
        print(f"   Mean: {stats['mean']:.3f}s")
        print(f"   Std:  {stats['std']:.3f}s")
        
        # Regional subset
        print("\n3. Regional Subset - East Africa, 1 month (5 iterations):")
        stats = client.benchmark_access(
            'region',
            n_iterations=5,
            region="east_africa",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        print(f"   Mean: {stats['mean']:.3f}s")
        print(f"   Std:  {stats['std']:.3f}s")


def example_8_data_validation():
    """Example 8: Data validation."""
    print("\n" + "="*80)
    print("EXAMPLE 8: Data Validation")
    print("="*80)
    
    with ChirpsZarrClient() as client:
        # Validate multiple dates
        dates = ["2024-01-01", "2024-06-15", "2024-12-31"]
        
        for date in dates:
            print(f"\nValidating {date}:")
            results = client.validate_data(date)
            print(f"   Has data: {results['has_data']}")
            print(f"   No negatives: {results['no_negatives']}")
            print(f"   Reasonable max: {results['reasonable_max']}")
            print(f"   Valid pixels: {results['valid_percent']:.1f}%")
            print(f"   Mean value: {results['mean_value']:.2f} mm/day")
            print(f"   ✓ PASSED" if results['all_passed'] else "   ✗ FAILED")


def main():
    """Run all examples."""
    print("\n" + "="*80)
    print("CHIRPS Zarr Client - Example Usage Scenarios")
    print("="*80)
    
    try:
        # example_1_basic_access()
        # example_2_spatial_subsetting()
        # example_3_temporal_aggregation()
        # example_4_parallel_access()
        # example_5_async_access()
        example_6_multi_region_analysis()
        # example_7_performance_benchmark()
        # example_8_data_validation()
        
        print("\n" + "="*80)
        print("All examples completed successfully!")
        print("="*80 + "\n")
        
    except FileNotFoundError as e:
        print(f"\n✗ Error: {e}")
        print("Make sure the Zarr store exists and bootstrap ingestion has completed.")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
