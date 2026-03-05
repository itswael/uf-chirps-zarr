"""
Enhanced ICASA Weather File Generator
Creates ICASA format weather files with CHIRPS and NASA POWER data
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional
import io
import pandas as pd
import numpy as np

from .nasa_power_config import nasa_power_config
from .elevation_provider import get_elevation

logger = logging.getLogger(__name__)


class EnhancedIcasaGenerator:
    """
    Generate ICASA format weather files from merged CHIRPS and NASA POWER data.
    Supports all meteorological variables with proper ICASA formatting.
    """
    
    def __init__(self):
        """Initialize the enhanced ICASA generator"""
        pass
    
    def generate_icasa_content(
        self,
        df: pd.DataFrame,
        lat: float,
        lon: float,
        site_code: str = "UFLC",
        source_description: str = "CHIRPS + NASA POWER"
    ) -> str:
        """
        Generate ICASA format weather file content from a DataFrame.
        
        Args:
            df: DataFrame with time column and weather variables
            lat: Latitude
            lon: Longitude
            site_code: Site identifier code (4 characters)
            source_description: Description of data source
            
        Returns:
            ICASA formatted file content as string
        """
        if df.empty:
            raise ValueError("DataFrame is empty")
        
        if 'time' not in df.columns:
            raise ValueError("DataFrame must have a 'time' column")
        
        # Get elevation for the location
        elevation = get_elevation(lat, lon)
        
        # Generate file content
        output = io.StringIO()
        
        # Write header with data source
        output.write(f"$WEATHER DATA : {source_description}\n\n")
        
        # Write variable descriptions for available variables
        available_vars = [col for col in df.columns if col != 'time']
        for var_code in available_vars:
            config = nasa_power_config.get_variable_config(var_code)
            if config:
                output.write(f"! {var_code:<6} {config['description']}\n")
        output.write("\n")
        
        # Write location header
        output.write("@ INSI   WTHLAT  WTHLONG   WELEV   TAV   AMP  REFHT  WNDHT\n")
        output.write(
            f"  {site_code:<4} {lat:>8.1f} {lon:>8.1f} {elevation:>7.2f} "
            f"{nasa_power_config.TAV:>5.1f} {nasa_power_config.AMP:>5.1f} "
            f"{nasa_power_config.REFHT:>6.0f} {nasa_power_config.WNDHT:>6.0f}\n\n"
        )
        
        # Write data header
        header_parts = ["@  DATE"]
        for var_code in available_vars:
            header_parts.append(f"{var_code:>8}")
        output.write("".join(header_parts) + "\n")
        
        # Write daily data
        for _, row in df.iterrows():
            try:
                # Parse date
                time_val = row['time']
                if isinstance(time_val, str):
                    dt = datetime.fromisoformat(time_val[:10])
                elif isinstance(time_val, pd.Timestamp):
                    dt = time_val.to_pydatetime()
                else:
                    dt = pd.to_datetime(time_val).to_pydatetime()
                
                # Format date as YYYYDDD
                day_of_year = dt.timetuple().tm_yday
                date_str = f"{dt.year}{day_of_year:03d}"
                
                # Build data line
                data_parts = [f"{date_str:>7}"]
                
                for var_code in available_vars:
                    value = row[var_code]
                    
                    # Handle missing values
                    if pd.isna(value):
                        config = nasa_power_config.get_variable_config(var_code)
                        if config:
                            value = config['default_value']
                        else:
                            value = -99.0
                    
                    # Get decimal places
                    config = nasa_power_config.get_variable_config(var_code)
                    decimal_places = config['decimal_places'] if config else 1
                    
                    formatted_val = f"{float(value):.{decimal_places}f}"
                    data_parts.append(f"{formatted_val:>8}")
                
                output.write("".join(data_parts) + "\n")
                
            except Exception as e:
                logger.error(f"Error formatting row: {e}")
                continue
        
        return output.getvalue()
    
    @staticmethod
    def create_filename(
        lat: float,
        lon: float,
        start_date: str,
        end_date: str,
        point_id: Optional[int] = None
    ) -> str:
        """
        Create standardized filename for ICASA weather file.
        
        Args:
            lat: Latitude
            lon: Longitude
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            point_id: Optional point identifier for multi-point downloads
            
        Returns:
            Filename string
        """
        # Clean up coordinates for filename
        lat_str = f"{abs(lat):.4f}{'N' if lat >= 0 else 'S'}"
        lon_str = f"{abs(lon):.4f}{'E' if lon >= 0 else 'W'}"
        
        if point_id is not None:
            return f"weather_point{point_id:04d}_{lat_str}_{lon_str}.WTH"
        else:
            return f"weather_{lat_str}_{lon_str}_{start_date.replace('-', '')}_{end_date.replace('-', '')}.WTH"


class EnhancedIcasaBatchGenerator:
    """Generate multiple ICASA files efficiently with merged data"""
    
    def __init__(self):
        """Initialize batch generator"""
        self.generator = EnhancedIcasaGenerator()
    
    async def generate_batch_from_merger(
        self,
        coordinates: List[tuple],
        start_date: str,
        end_date: str,
        merger,  # WeatherDataMerger instance
        rain_source: str = "both",
        site_code: str = "UFLC"
    ) -> Dict[str, str]:
        """
        Generate ICASA files for multiple coordinates using merged data.
        
        Args:
            coordinates: List of (lon, lat) tuples
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            merger: WeatherDataMerger instance
            rain_source: Rain data source - "chirps", "nasa_power", or "both"
            site_code: Site identifier code
            
        Returns:
            Dictionary mapping filenames to file contents
        """
        results = {}
        
        for i, (lon, lat) in enumerate(coordinates):
            try:
                # Get merged data for this coordinate
                df = await merger.merge_weather_data(
                    lat=lat,
                    lon=lon,
                    start_date=start_date,
                    end_date=end_date,
                    rain_source=rain_source,
                    include_solar=True,
                    include_met=True
                )
                
                # Generate filename
                filename = self.generator.create_filename(
                    lat=lat,
                    lon=lon,
                    start_date=start_date,
                    end_date=end_date,
                    point_id=i+1
                )
                
                # Generate ICASA content
                source_desc = self._get_source_description(rain_source)
                content = self.generator.generate_icasa_content(
                    df=df,
                    lat=lat,
                    lon=lon,
                    site_code=site_code,
                    source_description=source_desc
                )
                
                results[filename] = content
                
                logger.info(f"Generated ICASA file {i+1}/{len(coordinates)}: {filename}")
                
            except Exception as e:
                logger.error(f"Error generating ICASA file for point {i+1} ({lat}, {lon}): {e}")
                # Continue with other points
                continue
        
        return results
    
    def _get_source_description(self, rain_source: str) -> str:
        """Get source description based on rain source selection"""
        if rain_source == "chirps":
            return "CHIRPS + NASA POWER (Rain: CHIRPS)"
        elif rain_source == "nasa_power":
            return "NASA POWER"
        elif rain_source == "both":
            return "CHIRPS + NASA POWER (Dual Rain)"
        else:
            return "CHIRPS + NASA POWER"
