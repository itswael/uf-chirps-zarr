"""
ICASA Weather File Generator
Creates ICASA format weather files from precipitation data
Extensible for additional weather variables (solar radiation, temperature, etc.)
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import io

import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)


class IcasaWeatherGenerator:
    """Generate ICASA format weather files"""
    
    # Variable configurations - extensible for future variables
    VARIABLE_CONFIGS = {
        'precipitation': {
            'code': 'RAIN',
            'description': 'Precipitation Corrected (mm/day)',
            'units': 'mm/day',
            'decimal_places': 1,
            'default_value': 0.0
        },
        # Future variables can be added here:
        # 'solar_radiation': {
        #     'code': 'SRAD',
        #     'description': 'Solar Radiation (MJ/m2/day)',
        #     'units': 'MJ/m2/day',
        #     'decimal_places': 1,
        #     'default_value': 0.0
        # },
        # 'max_temperature': {
        #     'code': 'TMAX',
        #     'description': 'Maximum Temperature (°C)',
        #     'units': '°C',
        #     'decimal_places': 1,
        #     'default_value': 0.0
        # },
        # 'min_temperature': {
        #     'code': 'TMIN',
        #     'description': 'Minimum Temperature (°C)',
        #     'units': '°C',
        #     'decimal_places': 1,
        #     'default_value': 0.0
        # }
    }
    
    def __init__(self, dataset: xr.Dataset):
        """
        Initialize generator with xarray dataset.
        
        Args:
            dataset: Xarray dataset containing weather variables
        """
        self.dataset = dataset
    
    def generate_icasa_file(
        self,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str,
        variables: Optional[List[str]] = None,
        site_code: str = "UFLC"
    ) -> str:
        """
        Generate ICASA format weather file content.
        
        Args:
            lat: Latitude
            lon: Longitude
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            variables: List of variables to include (default: all available)
            site_code: Site identifier code
            
        Returns:
            ICASA formatted file content as string
        """
        # Select data for location and date range
        data = self._extract_data(lat, lon, start_date, end_date)
        
        # Determine which variables to include
        if variables is None:
            variables = self._get_available_variables()
        
        # Generate file content
        output = io.StringIO()
        
        # Write header
        output.write(f"$WEATHER DATA: {site_code}\n\n")
        
        # Write variable descriptions
        for var in variables:
            if var in self.VARIABLE_CONFIGS:
                config = self.VARIABLE_CONFIGS[var]
                output.write(f"! {config['code']}     {config['description']}\n")
        output.write("\n")
        
        # Write location header
        output.write(f"@ INSI   WTHLAT  WTHLONG\n")
        output.write(f"  {site_code}     {lat:.1f}    {lon:.1f}\n\n")
        
        # Write data header
        var_codes = [self.VARIABLE_CONFIGS[var]['code'] for var in variables if var in self.VARIABLE_CONFIGS]
        output.write(f"@  DATE   {'   '.join(var_codes)}\n")
        
        # Write daily data
        time_values = data['time']
        for i, time_val in enumerate(time_values):
            dt = datetime.fromisoformat(str(time_val)[:10])
            date_str = dt.strftime("%Y%j")  # YYYYDDD format
            
            # Collect values for each variable
            values = []
            for var in variables:
                if var in data and var in self.VARIABLE_CONFIGS:
                    config = self.VARIABLE_CONFIGS[var]
                    val = data[var][i]
                    
                    if np.isnan(val):
                        val = config['default_value']
                    
                    formatted_val = f"{float(val):.{config['decimal_places']}f}"
                    values.append(f"{formatted_val:>6}")
            
            output.write(f"{date_str}   {''.join(values)}\n")
        
        return output.getvalue()
    
    def _extract_data(
        self,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        Extract data for specified location and date range.
        
        Args:
            lat: Latitude
            lon: Longitude
            start_date: Start date
            end_date: End date
            
        Returns:
            Dictionary with time and variable data
        """
        # Select spatial point first (faster), then time range
        data = self.dataset.sel(
            longitude=lon,
            latitude=lat,
            method='nearest'
        ).sel(
            time=slice(start_date, end_date)
        )
        
        # Compute all data at once
        data_computed = data.compute()
        
        result = {
            'time': data_computed.time.values
        }
        
        # Extract each variable
        for var in self.dataset.data_vars:
            if var in data_computed:
                result[str(var)] = data_computed[var].values
        
        return result
    
    def _get_available_variables(self) -> List[str]:
        """Get list of available variables in dataset that are configured."""
        available = []
        for var in self.dataset.data_vars:
            if var in self.VARIABLE_CONFIGS:
                available.append(var)
        return available
    
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
            start_date: Start date
            end_date: End date
            point_id: Optional point identifier for multi-point downloads
            
        Returns:
            Filename string
        """
        if point_id is not None:
            return f"weather_point{point_id:04d}_{lat:.4f}_{lon:.4f}_{start_date}_{end_date}.txt"
        else:
            return f"weather_{lat}_{lon}_{start_date}_{end_date}.txt"


class IcasaBatchGenerator:
    """Generate multiple ICASA files efficiently"""
    
    def __init__(self, dataset: xr.Dataset):
        """
        Initialize batch generator with xarray dataset.
        
        Args:
            dataset: Xarray dataset containing weather variables
        """
        self.generator = IcasaWeatherGenerator(dataset)
    
    def generate_batch(
        self,
        coordinates: List[tuple],
        start_date: str,
        end_date: str,
        variables: Optional[List[str]] = None,
        site_code: str = "UFLC"
    ) -> Dict[str, str]:
        """
        Generate ICASA files for multiple coordinates.
        
        Args:
            coordinates: List of (lon, lat) tuples
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            variables: List of variables to include
            site_code: Site identifier code
            
        Returns:
            Dictionary mapping filenames to file contents
        """
        results = {}
        
        for i, (lon, lat) in enumerate(coordinates):
            try:
                filename = IcasaWeatherGenerator.create_filename(
                    lat, lon, start_date, end_date, point_id=i+1
                )
                
                content = self.generator.generate_icasa_file(
                    lat=lat,
                    lon=lon,
                    start_date=start_date,
                    end_date=end_date,
                    variables=variables,
                    site_code=site_code
                )
                
                results[filename] = content
                
            except Exception as e:
                logger.error(f"Error generating ICASA file for point {i+1} ({lat}, {lon}): {e}")
                # Continue with other points
                continue
        
        return results
