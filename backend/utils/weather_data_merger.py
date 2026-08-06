"""
Weather Data Merger
Combines CHIRPS and NASA POWER data with resolution handling
"""
import logging
from datetime import date
from typing import Optional, Dict
import pandas as pd
import numpy as np
import xarray as xr

from .nasa_power_config import nasa_power_config
from .nasa_power_fetcher import get_fetcher

logger = logging.getLogger(__name__)


class WeatherDataMerger:
    """Merge CHIRPS precipitation and NASA POWER meteorological data"""
    
    def __init__(
        self,
        chirps_dataset: xr.Dataset,
        power_dataset_overrides: Optional[Dict[str, xr.Dataset]] = None
    ):
        """
        Initialize merger with CHIRPS dataset.
        
        Args:
            chirps_dataset: Xarray dataset containing CHIRPS precipitation data
        """
        self.chirps_dataset = chirps_dataset
        self.nasa_fetcher = get_fetcher()
        self.power_dataset_overrides = power_dataset_overrides
    
    async def get_chirps_data(
        self,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        Extract CHIRPS precipitation data for a location.
        
        Args:
            lat: Latitude
            lon: Longitude
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame with time and RAIN columns
        """
        try:
            # Select spatial point first (faster), then time range
            data = self.chirps_dataset.sel(
                longitude=lon,
                latitude=lat,
                method='nearest'
            ).sel(
                time=slice(start_date, end_date)
            )
            
            # Compute data
            data_computed = data.compute()
            
            # Convert to DataFrame
            df = data_computed.to_dataframe().reset_index()
            
            # Rename precipitation to RAIN (CHIRPS rain)
            df = df.rename(columns={'precipitation': 'RAIN'})
            
            # Round to 1 decimal place
            df['RAIN'] = df['RAIN'].round(1)
            
            # Keep only time and RAIN
            df = df[['time', 'RAIN']]
            
            logger.info(f"Extracted CHIRPS data: {len(df)} days")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting CHIRPS data: {e}")
            raise
    
    async def merge_weather_data(
        self,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str,
        rain_source: str = "both",
        include_solar: bool = True,
        include_met: bool = True
    ) -> pd.DataFrame:
        """
        Merge CHIRPS and NASA POWER data.
        
        Args:
            lat: Latitude
            lon: Longitude
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            rain_source: Rain data source - "chirps", "nasa_power", or "both"
            include_solar: Include solar radiation
            include_met: Include meteorological variables
            
        Returns:
            Merged DataFrame with all weather variables
        """
        try:
            # Convert dates to date objects
            start = pd.to_datetime(start_date).date()
            end = pd.to_datetime(end_date).date()
            
            df_merged = None
            
            # Fetch CHIRPS data if needed
            if rain_source in ["chirps", "both"]:
                df_chirps = await self.get_chirps_data(lat, lon, start_date, end_date)
                df_merged = df_chirps
            
            # Fetch NASA POWER data
            df_nasa = await self.nasa_fetcher.fetch_nasa_power_data(
                latitude=lat,
                longitude=lon,
                start_date=start,
                end_date=end,
                include_solar=include_solar,
                include_met=include_met,
                dataset_overrides=self.power_dataset_overrides
            )
            
            # Handle rain data based on source selection
            if rain_source == "nasa_power":
                # Use NASA POWER rain as RAIN (remove RAIN1, rename to RAIN)
                if 'RAIN1' in df_nasa.columns:
                    df_nasa = df_nasa.rename(columns={'RAIN1': 'RAIN'})
                df_merged = df_nasa
                
            elif rain_source == "chirps":
                # Use CHIRPS rain as RAIN and add other NASA POWER variables.
                if df_merged is not None:
                    # Drop RAIN1 from NASA data if present
                    if 'RAIN1' in df_nasa.columns:
                        df_nasa = df_nasa.drop(columns=['RAIN1'])
                    
                    # Merge with NASA POWER data
                    df_merged = pd.merge(df_merged, df_nasa, on="time", how="inner")
                else:
                    # Fallback if CHIRPS data couldn't be fetched
                    if 'RAIN1' in df_nasa.columns:
                        df_nasa = df_nasa.rename(columns={'RAIN1': 'RAIN'})
                    df_merged = df_nasa
                    
            elif rain_source == "both":
                # Keep both RAIN (CHIRPS) and RAIN1 (NASA POWER)
                if df_merged is not None:
                    df_merged = pd.merge(df_merged, df_nasa, on="time", how="inner")
                else:
                    df_merged = df_nasa
            
            # Ensure time column is present
            if df_merged is None or df_merged.empty:
                raise ValueError("No data could be retrieved")
            
            # Drop lat/lon columns if present
            cols_to_drop = ['lat', 'lon', 'latitude', 'longitude']
            df_merged = df_merged.drop(columns=[c for c in cols_to_drop if c in df_merged.columns])
            
            logger.info(
                f"Merged weather data: {len(df_merged)} days, "
                f"variables: {[c for c in df_merged.columns if c != 'time']}"
            )
            
            return df_merged
            
        except Exception as e:
            logger.error(f"Error merging weather data: {e}")
            raise
    
    async def get_available_variables(
        self,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str
    ) -> dict:
        """
        Get metadata about available variables for a location.
        
        Returns:
            Dictionary with available variables and their sources
        """
        try:
            # Get a sample of merged data
            df = await self.merge_weather_data(
                lat, lon, start_date, end_date,
                rain_source="both",
                include_solar=True,
                include_met=True
            )
            
            # Identify variable sources
            available_vars = {}
            for col in df.columns:
                if col == 'time':
                    continue
                
                config = nasa_power_config.get_variable_config(col)
                if config:
                    available_vars[col] = {
                        'description': config['description'],
                        'units': config['units'],
                        'source': config['source']
                    }
            
            return available_vars
            
        except Exception as e:
            logger.error(f"Error getting available variables: {e}")
            return {}
