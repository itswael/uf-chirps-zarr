"""
Example: Bootstrap ingestion with a small date range for testing.

This demonstrates how to run bootstrap ingestion programmatically.
"""

from datetime import date
from src.config import get_config
from src.orchestration.bootstrap_ingestion import BootstrapOrchestrator


def main():
    """Run a small bootstrap example."""
    # Get configuration
    config = get_config()
    
    print("CHIRPS Zarr Bootstrap Example")
    print("=" * 80)
    print(f"This will download and ingest CHIRPS data for a 7-day test period")
    print(f"Zarr store will be created at: {config.ZARR_STORE_PATH}")
    print("=" * 80)
    
    # Define a small date range for testing (1 week)
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 7)
    
    print(f"\nDate range: {start_date} to {end_date}")
    print(f"Total days: {(end_date - start_date).days + 1}")
    
    # Create orchestrator
    orchestrator = BootstrapOrchestrator(
        config=config,
        start_date=start_date,
        end_date=end_date
    )
    
    # Run bootstrap ingestion
    try:
        total, successful, failed = orchestrator.run()
        
        print("\n" + "=" * 80)
        print("BOOTSTRAP COMPLETE")
        print("=" * 80)
        print(f"Total days processed: {total}")
        print(f"Successfully ingested: {successful}")
        print(f"Failed: {failed}")
        
        if successful > 0:
            print(f"\nZarr store created at: {config.ZARR_STORE_PATH}")
            print("You can now query this data using xarray:")
            print(f"  import xarray as xr")
            print(f"  ds = xr.open_zarr('{config.ZARR_STORE_PATH}')")
            print(f"  print(ds)")
        
    except Exception as e:
        print(f"\nError during bootstrap: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
