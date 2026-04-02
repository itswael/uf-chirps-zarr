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
from .elevation_provider import get_elevation_provider

logger = logging.getLogger(__name__)


class EnhancedIcasaGenerator:
    """
    Generate ICASA format weather files from merged CHIRPS and NASA POWER data.
    Supports all meteorological variables with proper ICASA formatting.
    """
    
    def __init__(self):
        """Initialize the enhanced ICASA generator"""
        pass

    # Fixed-width layout for location section to keep columns aligned.
    _SITE_COL_WIDTHS = {
        'INSI': 6,
        'WTHLAT': 9,
        'WTHLONG': 10,
        'WELEV': 10,
        'TAV': 7,
        'AMP': 6,
        'REFHT': 7,
        'WNDHT': 7,
    }

    @classmethod
    def _format_site_header(cls) -> str:
        w = cls._SITE_COL_WIDTHS
        return (
            f"@ {'INSI':<{w['INSI']}}"
            f"{'WTHLAT':>{w['WTHLAT']}}"
            f"{'WTHLONG':>{w['WTHLONG']}}"
            f"{'WELEV':>{w['WELEV']}}"
            f"{'TAV':>{w['TAV']}}"
            f"{'AMP':>{w['AMP']}}"
            f"{'REFHT':>{w['REFHT']}}"
            f"{'WNDHT':>{w['WNDHT']}}"
        )

    @classmethod
    def _format_site_row(
        cls,
        site_code: str,
        lat: float,
        lon: float,
        elevation: float,
        tav: float,
        amp: float,
        refht: float,
        wndht: float,
    ) -> str:
        w = cls._SITE_COL_WIDTHS
        return (
            f"  {site_code:<{w['INSI']}}"
            f"{lat:>{w['WTHLAT']}.1f}"
            f"{lon:>{w['WTHLONG']}.1f}"
            f"{elevation:>{w['WELEV']}.1f}"
            f"{tav:>{w['TAV']}.1f}"
            f"{amp:>{w['AMP']}.1f}"
            f"{refht:>{w['REFHT']}.0f}"
            f"{wndht:>{w['WNDHT']}.0f}"
        )

    @staticmethod
    def _compute_tav_amp(df: pd.DataFrame) -> tuple[float, float]:
        """
        Compute ICASA TAV and AMP values.

        Rules:
        - If selected span is less than 30 days, return -99.0 for both TAV and AMP.
        - TAV is mean temperature over the selected span.
        - AMP is computed from monthly aggregates when available.
        """
        if df.empty or 'time' not in df.columns:
            return -99.0, -99.0

        time_index = pd.to_datetime(df['time'], errors='coerce').dropna()
        if time_index.empty:
            return -99.0, -99.0

        span_days = int((time_index.max() - time_index.min()).days) + 1
        if span_days < 30:
            return -99.0, -99.0

        temp_series = None
        if 'T2M' in df.columns:
            temp_series = pd.to_numeric(df['T2M'], errors='coerce')
        elif 'TMAX' in df.columns and 'TMIN' in df.columns:
            tmax = pd.to_numeric(df['TMAX'], errors='coerce')
            tmin = pd.to_numeric(df['TMIN'], errors='coerce')
            temp_series = (tmax + tmin) / 2.0

        if temp_series is None:
            return -99.0, -99.0

        temp_series = temp_series.dropna()
        if temp_series.empty:
            return -99.0, -99.0

        tav = float(temp_series.mean())

        temp_df = pd.DataFrame({
            'time': pd.to_datetime(df['time'], errors='coerce'),
            'temp_mean': pd.to_numeric(temp_series.reindex(df.index), errors='coerce'),
        }).dropna(subset=['time', 'temp_mean'])

        if temp_df.empty:
            return round(tav, 1), -99.0

        month_key = temp_df['time'].dt.to_period('M')
        monthly_mean = temp_df.groupby(month_key)['temp_mean'].mean()

        amp = -99.0
        if 'TMAX' in df.columns:
            tmax_df = pd.DataFrame({
                'time': pd.to_datetime(df['time'], errors='coerce'),
                'tmax': pd.to_numeric(df['TMAX'], errors='coerce'),
            }).dropna(subset=['time', 'tmax'])
            if not tmax_df.empty:
                monthly_tmax = tmax_df.groupby(tmax_df['time'].dt.to_period('M'))['tmax'].mean()
                common_months = monthly_tmax.index.intersection(monthly_mean.index)
                if len(common_months) > 0:
                    # User-requested definition: AMP = 1/2 * (monthly max - monthly avg)
                    amp_values = 0.5 * (monthly_tmax.loc[common_months] - monthly_mean.loc[common_months])
                    amp = float(amp_values.mean())

        if amp == -99.0 and len(monthly_mean) > 1:
            # Fallback amplitude using monthly mean temperature range.
            amp = float((monthly_mean.max() - monthly_mean.min()) / 2.0)

        return round(tav, 1), (round(amp, 1) if amp != -99.0 else -99.0)
    
    def generate_icasa_content(
        self,
        df: pd.DataFrame,
        lat: float,
        lon: float,
        site_code: str = "UFLC",
        source_description: str = "CHIRPS + NASA POWER",
        selected_variables: Optional[List[str]] = None,
        elevation: Optional[float] = None,
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
        
        # Get elevation for the location (or use precomputed value for batch runs)
        if elevation is None:
            elevation = get_elevation(lat, lon)
        tav, amp = self._compute_tav_amp(df)
        
        # Generate file content
        output = io.StringIO()
        
        # Write header with data source
        output.write(f"$WEATHER DATA : {source_description}\n\n")
        
        # Write variable descriptions for available variables
        available_vars = [col for col in df.columns if col != 'time']
        if selected_variables:
            selected_set = set(selected_variables)
            available_vars = [var for var in available_vars if var in selected_set]

        if not available_vars:
            raise ValueError("No matching weather variables available for ICASA output")

        for var_code in available_vars:
            config = nasa_power_config.get_variable_config(var_code)
            if config:
                output.write(f"! {var_code:<6} {config['description']}\n")
        output.write("\n")
        
        # Write location header and row using identical fixed-width layout.
        output.write(f"{self._format_site_header()}\n")
        output.write(
            f"{self._format_site_row(site_code, lat, lon, elevation, tav, amp, nasa_power_config.REFHT, nasa_power_config.WNDHT)}\n\n"
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
        point_id: Optional[str] = None
    ) -> str:
        """
        Create standardized filename for ICASA weather file.
        If point_id is provided (from shapefile/geojson), use it directly as filename.
        
        Args:
            lat: Latitude
            lon: Longitude
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            point_id: Optional point identifier from data source (shapefile, geojson, etc.)
            
        Returns:
            Filename string
        """
        # If a point ID is provided from the data source, use it directly as filename
        if point_id is not None:
            return f"{point_id}.WTH"
        
        # Otherwise, create filename from coordinates
        lat_str = f"{abs(lat):.4f}{'N' if lat >= 0 else 'S'}"
        lon_str = f"{abs(lon):.4f}{'E' if lon >= 0 else 'W'}"
        
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
        site_code: str = "UFLC",
        selected_variables: Optional[List[str]] = None,
        point_ids_mapping: Optional[Dict[int, str]] = None,
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
            selected_variables: Optional list of variables to include in output
            point_ids_mapping: Optional dict mapping coordinate index to point ID (from shapefile/geojson)
            
        Returns:
            Dictionary mapping filenames to file contents
        """
        total_points = len(coordinates)
        logger.info(f"Starting parallel ICASA generation for {total_points} points with {self.max_workers} workers")
        
        # Create semaphore to limit concurrent operations
        semaphore = asyncio.Semaphore(self.max_workers)

        # Pre-compute elevations in one vectorized batch to reduce interpolation overhead.
        elevation_provider = get_elevation_provider()
        elevation_values = elevation_provider.get_elevations_batch(
            [(lat, lon) for lon, lat in coordinates]
        )
        
        # Create tasks for all coordinates
        tasks = []
        for i, (lon, lat) in enumerate(coordinates):
            # Get point ID from mapping, or use index-based ID as fallback
            point_id = point_ids_mapping.get(i) if point_ids_mapping else None
            
            task = self._generate_single_point(
                semaphore=semaphore,
                point_id=point_id,
                lon=lon,
                lat=lat,
                elevation=elevation_values[i],
                start_date=start_date,
                end_date=end_date,
                merger=merger,
                rain_source=rain_source,
                site_code=site_code,
                selected_variables=selected_variables,
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
        point_id: Optional[str],
        lon: float,
        lat: float,
        elevation: float,
        start_date: str,
        end_date: str,
        merger,
        rain_source: str,
        site_code: str,
        selected_variables: Optional[List[str]],
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
                
                # Generate filename using point ID if available
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
                    source_description=source_desc,
                    selected_variables=selected_variables,
                    elevation=elevation,
                )
                
                # Log progress for point-based IDs
                if point_id is not None:
                    logger.debug(f"Generated ICASA file for point {point_id}")
                
                return (filename, content)
                
            except Exception as e:
                logger.error(f"Error generating ICASA file for point ({lat}, {lon}): {e}")
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
