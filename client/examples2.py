"""
CHIRPS Zarr Data Explorer

Simple script to extract precipitation data for a specific location and date range.
Configure the parameters below and run to get raw precipitation values.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional
import pandas as pd
import xarray as xr
import numpy as np


class ChirpsDataExplorer:
    """Extract precipitation data from CHIRPS Zarr store."""
    
    def __init__(self, zarr_path: Optional[Path] = None):
        """
        Initialize the data explorer.
        
        Args:
            zarr_path: Path to CHIRPS Zarr store
        """
        if zarr_path is None:
            # Default path - adjust if needed
            zarr_path = Path(__file__).parent.parent / "data" / "zarr" / "chirps_v3.0_daily_precip_v1.0.zarr"
        
        self.zarr_path = Path(zarr_path)
        if not self.zarr_path.exists():
            raise FileNotFoundError(f"Zarr store not found: {self.zarr_path}")
        
        self._dataset = None
    
    def open(self):
        """Open the Zarr dataset."""
        if self._dataset is None:
            print(f"Opening Zarr store: {self.zarr_path}")
            self._dataset = xr.open_zarr(self.zarr_path)
            print(f"Dataset loaded: {dict(self._dataset.sizes)}")
        return self._dataset
    
    def close(self):
        """Close the dataset."""
        if self._dataset is not None:
            self._dataset.close()
            self._dataset = None
    
    def get_precipitation_data(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        method: str = 'nearest'
    ) -> pd.DataFrame:
        """
        Extract precipitation data for a specific location and date range.
        
        Args:
            latitude: Latitude in decimal degrees (-90 to 90)
            longitude: Longitude in decimal degrees (-180 to 180)
            start_date: Start date in format 'YYYY-MM-DD'
            end_date: End date in format 'YYYY-MM-DD'
            method: Selection method ('nearest' or 'interp')
                   - 'nearest': Use nearest grid point
                   - 'interp': Interpolate between grid points
        
        Returns:
            pandas DataFrame with columns: date, precipitation_mm
        """
        # Open dataset
        ds = self.open()
        
        # Convert date strings to datetime
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        print(f"\nExtracting data for:")
        print(f"  Location: ({latitude}, {longitude})")
        print(f"  Date range: {start_date} to {end_date}")
        print(f"  Selection method: {method}")
        
        # Select spatial point
        if method == 'nearest':
            # Use nearest grid point
            data = ds.sel(
                latitude=latitude,
                longitude=longitude,
                method='nearest'
            )
            actual_lat = float(data.latitude.values)
            actual_lon = float(data.longitude.values)
            print(f"  Nearest grid point: ({actual_lat:.4f}, {actual_lon:.4f})")
        elif method == 'interp':
            # Interpolate between grid points
            data = ds.interp(
                latitude=latitude,
                longitude=longitude,
                method='linear'
            )
            print(f"  Using interpolated values")
        else:
            raise ValueError(f"Unknown method: {method}. Use 'nearest' or 'interp'")
        
        # Select temporal range
        data = data.sel(time=slice(start_dt, end_dt))
        
        # Extract precipitation values
        precip_data = data['precipitation']
        
        # Convert to pandas DataFrame
        df = pd.DataFrame({
            'date': pd.to_datetime(precip_data.time.values),
            'precipitation_mm': precip_data.values
        })
        
        # Add summary statistics
        print(f"\nData summary:")
        print(f"  Total days: {len(df)}")
        print(f"  Valid values: {(~np.isnan(df['precipitation_mm'])).sum()}")
        print(f"  Missing values: {np.isnan(df['precipitation_mm']).sum()}")
        print(f"  Mean precipitation: {df['precipitation_mm'].mean():.2f} mm/day")
        print(f"  Max precipitation: {df['precipitation_mm'].max():.2f} mm/day")
        print(f"  Total precipitation: {df['precipitation_mm'].sum():.2f} mm")
        
        return df
    
    def get_area_average(
        self,
        lat_min: float,
        lat_max: float,
        lon_min: float,
        lon_max: float,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        Extract area-averaged precipitation data for a bounding box.
        
        Args:
            lat_min: Minimum latitude
            lat_max: Maximum latitude
            lon_min: Minimum longitude
            lon_max: Maximum longitude
            start_date: Start date in format 'YYYY-MM-DD'
            end_date: End date in format 'YYYY-MM-DD'
        
        Returns:
            pandas DataFrame with columns: date, precipitation_mm_avg
        """
        # Open dataset
        ds = self.open()
        
        # Convert date strings to datetime
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        print(f"\nExtracting area-averaged data for:")
        print(f"  Bounding box: ({lat_min}, {lon_min}) to ({lat_max}, {lon_max})")
        print(f"  Date range: {start_date} to {end_date}")
        
        # Select spatial region
        data = ds.sel(
            latitude=slice(lat_max, lat_min),  # Note: reversed for descending coords
            longitude=slice(lon_min, lon_max),
            time=slice(start_dt, end_dt)
        )
        
        grid_shape = (data.dims['latitude'], data.dims['longitude'])
        print(f"  Grid cells: {grid_shape[0]} x {grid_shape[1]} = {grid_shape[0] * grid_shape[1]} cells")
        
        # Calculate spatial average
        precip_avg = data['precipitation'].mean(dim=['latitude', 'longitude'])
        
        # Convert to DataFrame
        df = pd.DataFrame({
            'date': pd.to_datetime(precip_avg.time.values),
            'precipitation_mm_avg': precip_avg.values
        })
        
        # Add summary statistics
        print(f"\nData summary:")
        print(f"  Total days: {len(df)}")
        print(f"  Mean precipitation: {df['precipitation_mm_avg'].mean():.2f} mm/day")
        print(f"  Max precipitation: {df['precipitation_mm_avg'].max():.2f} mm/day")
        
        return df
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def example_single_point():
    """Example: Extract data for a single point."""
    print("=" * 80)
    print("EXAMPLE 1: Single Point Extraction")
    print("=" * 80)
    
    # Configure parameters
    latitude = 42.0      # Latitude (Iowa, USA example)
    longitude = -93.5    # Longitude
    start_date = "2025-01-01"
    end_date = "2025-01-10"
    
    with ChirpsDataExplorer() as explorer:
        df = explorer.get_precipitation_data(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            method='nearest'  # Use nearest grid point
        )
        
        # Display results
        print("\nRaw precipitation data:")
        print(df.to_string(index=False))
        
        # Optional: Save to CSV
        # output_file = "precipitation_data.csv"
        # df.to_csv(output_file, index=False)
        # print(f"\nData saved to: {output_file}")
    
    return df


def example_interpolated_point():
    """Example: Extract data with interpolation."""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Interpolated Point Extraction")
    print("=" * 80)
    
    # Configure parameters
    latitude = 42.15     # Exact coordinates (not on grid)
    longitude = -93.75
    start_date = "2025-01-01"
    end_date = "2025-01-10"
    
    with ChirpsDataExplorer() as explorer:
        df = explorer.get_precipitation_data(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            method='interp'  # Interpolate between grid points
        )
        
        print("\nInterpolated precipitation data:")
        print(df.to_string(index=False))
    
    return df


def example_area_average():
    """Example: Extract area-averaged data."""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Area-Averaged Extraction")
    print("=" * 80)
    
    # Configure bounding box (Iowa, USA example)
    lat_min = 40.5
    lat_max = 43.5
    lon_min = -96.5
    lon_max = -90.0
    start_date = "2025-01-01"
    end_date = "2025-01-10"
    
    with ChirpsDataExplorer() as explorer:
        df = explorer.get_area_average(
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            start_date=start_date,
            end_date=end_date
        )
        
        print("\nArea-averaged precipitation data:")
        print(df.to_string(index=False))
    
    return df


def custom_query():
    """Custom query - modify parameters here."""
    print("\n" + "=" * 80)
    print("CUSTOM QUERY")
    print("=" * 80)
    
    # ========================================================================
    # CONFIGURE YOUR PARAMETERS HERE
    # ========================================================================
    latitude = 42.0
    longitude = -93.5
    start_date = "2025-01-01"
    end_date = "2025-01-31"
    method = 'nearest'  # 'nearest' or 'interp'
    # ========================================================================
    
    with ChirpsDataExplorer() as explorer:
        df = explorer.get_precipitation_data(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            method=method
        )
        
        print("\nPrecipitation data:")
        print(df.to_string(index=False))
        
        # Optionally save to CSV
        output_file = "custom_precipitation_data.csv"
        df.to_csv(output_file, index=False)
        print(f"\nData saved to: {output_file}")
    
    return df


def main():
    """Run examples."""
    print("\nCHIRPS Zarr Data Explorer")
    print("=" * 80)
    
    try:
        # Run examples
        # Uncomment the examples you want to run:
        
        # example_single_point()
        # example_interpolated_point()
        # example_area_average()
        custom_query()
        
        print("\n" + "=" * 80)
        print("Exploration complete!")
        print("=" * 80)
        
    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}")
        print("Make sure the Zarr store exists at the expected location.")
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
