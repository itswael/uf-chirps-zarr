"""
Enhanced ICASA Weather File Generator
Creates ICASA format weather files with CHIRPS and NASA POWER data
"""
import asyncio
import logging
import os
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
    """Generate multiple ICASA files efficiently with merged data using parallel processing"""
    
    def __init__(self, max_workers: Optional[int] = None):
        """
        Initialize batch generator with parallel processing support.
        
        Args:
            max_workers: Maximum concurrent workers. If None, uses min(32, CPU count + 4)
        """
        self.generator = EnhancedIcasaGenerator()
        
        # Determine optimal number of workers
        if max_workers is None:
            # Default: min(32, CPU count + 4) - balances performance and resource usage
            cpu_count = os.cpu_count() or 4
            self.max_workers = min(32, cpu_count + 4)
        else:
            self.max_workers = max_workers
        
        logger.info(f"Initialized batch generator with {self.max_workers} max workers")
    
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
        Generate ICASA files for multiple coordinates using merged data in parallel.
        
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
        total_points = len(coordinates)
        logger.info(f"Starting parallel ICASA generation for {total_points} points with {self.max_workers} workers")
        
        # Create semaphore to limit concurrent operations
        semaphore = asyncio.Semaphore(self.max_workers)
        
        # Create tasks for all coordinates
        tasks = []
        for i, (lon, lat) in enumerate(coordinates):
            task = self._generate_single_point(
                semaphore=semaphore,
                point_id=i+1,
                lon=lon,
                lat=lat,
                start_date=start_date,
                end_date=end_date,
                merger=merger,
                rain_source=rain_source,
                site_code=site_code,
                total_points=total_points
            )
            tasks.append(task)
        
        # Execute all tasks in parallel with progress logging
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect successful results
        results = {}
        successful = 0
        failed = 0
        
        for result in results_list:
            if isinstance(result, Exception):
                failed += 1
                logger.error(f"Task failed with exception: {result}")
            elif result is not None:
                filename, content = result
                results[filename] = content
                successful += 1
            else:
                failed += 1
        
        logger.info(
            f"Parallel ICASA generation complete: {successful} successful, "
            f"{failed} failed out of {total_points} total"
        )
        
        return results
    
    async def _generate_single_point(
        self,
        semaphore: asyncio.Semaphore,
        point_id: int,
        lon: float,
        lat: float,
        start_date: str,
        end_date: str,
        merger,
        rain_source: str,
        site_code: str,
        total_points: int
    ) -> Optional[tuple]:
        """
        Generate ICASA file for a single point with semaphore control.
        
        Returns:
            Tuple of (filename, content) or None if failed
        """
        async with semaphore:
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
                    point_id=point_id
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
                
                # Log progress periodically (every 10 points or on key milestones)
                if point_id % 10 == 0 or point_id == total_points:
                    logger.info(f"Progress: {point_id}/{total_points} points completed")
                
                return (filename, content)
                
            except Exception as e:
                logger.error(f"Error generating ICASA file for point {point_id} ({lat}, {lon}): {e}")
                return None
    
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
