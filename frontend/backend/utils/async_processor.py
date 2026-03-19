"""
Async Multi-Point Processor
Efficiently processes multiple coordinates in parallel using asyncio and thread pools
"""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import List, Dict, Tuple, Optional
import io
import zipfile

import xarray as xr

from .icasa_generator import IcasaWeatherGenerator

logger = logging.getLogger(__name__)


class AsyncMultiPointProcessor:
    """Process multiple points asynchronously for optimal performance"""
    
    def __init__(
        self,
        dataset: xr.Dataset,
        max_workers: Optional[int] = None,
        use_processes: bool = False
    ):
        """
        Initialize async processor.
        
        Args:
            dataset: Xarray dataset containing weather data
            max_workers: Maximum number of workers (default: min(32, CPU count + 4))
            use_processes: Use ProcessPoolExecutor instead of ThreadPoolExecutor
        """
        self.dataset = dataset
        self.max_workers = max_workers
        self.use_processes = use_processes
    
    async def process_coordinates_batch(
        self,
        coordinates: List[Tuple[float, float]],
        start_date: str,
        end_date: str,
        variables: Optional[List[str]] = None,
        site_code: str = "UFLC",
        batch_size: int = 50
    ) -> Dict[str, str]:
        """
        Process multiple coordinates in batches asynchronously.
        
        Args:
            coordinates: List of (lon, lat) tuples
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            variables: List of variables to include
            site_code: Site identifier code
            batch_size: Number of coordinates to process in each batch
            
        Returns:
            Dictionary mapping filenames to file contents
        """
        results = {}
        total_points = len(coordinates)
        
        logger.info(f"Processing {total_points} coordinates in batches of {batch_size}")
        
        # Process coordinates in batches
        for i in range(0, total_points, batch_size):
            batch = coordinates[i:i + batch_size]
            batch_start_id = i + 1
            
            logger.info(f"Processing batch {i // batch_size + 1}/{(total_points + batch_size - 1) // batch_size}")
            
            batch_results = await self._process_batch(
                batch=batch,
                start_date=start_date,
                end_date=end_date,
                variables=variables,
                site_code=site_code,
                start_id=batch_start_id
            )
            
            results.update(batch_results)
        
        logger.info(f"Successfully processed {len(results)}/{total_points} coordinates")
        return results
    
    async def _process_batch(
        self,
        batch: List[Tuple[float, float]],
        start_date: str,
        end_date: str,
        variables: Optional[List[str]],
        site_code: str,
        start_id: int
    ) -> Dict[str, str]:
        """Process a single batch of coordinates."""
        loop = asyncio.get_event_loop()
        
        # Choose executor type
        if self.use_processes:
            executor = ProcessPoolExecutor(max_workers=self.max_workers)
        else:
            executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        try:
            # Create tasks for each coordinate
            tasks = []
            for idx, (lon, lat) in enumerate(batch):
                point_id = start_id + idx
                task = loop.run_in_executor(
                    executor,
                    self._process_single_point,
                    lat,
                    lon,
                    start_date,
                    end_date,
                    variables,
                    site_code,
                    point_id
                )
                tasks.append(task)
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Collect successful results
            batch_results = {}
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Error processing point: {result}")
                elif result is not None:
                    if isinstance(result, tuple) and len(result) == 2:
                        filename, content = result
                        batch_results[filename] = content
            
            return batch_results
            
        finally:
            executor.shutdown(wait=False)
    
    def _process_single_point(
        self,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str,
        variables: Optional[List[str]],
        site_code: str,
        point_id: int
    ) -> Optional[Tuple[str, str]]:
        """
        Process a single coordinate point.
        
        Returns:
            Tuple of (filename, content) or None on error
        """
        try:
            generator = IcasaWeatherGenerator(self.dataset)
            
            filename = IcasaWeatherGenerator.create_filename(
                lat=lat,
                lon=lon,
                start_date=start_date,
                end_date=end_date,
                point_id=point_id
            )
            
            content = generator.generate_icasa_file(
                lat=lat,
                lon=lon,
                start_date=start_date,
                end_date=end_date,
                variables=variables,
                site_code=site_code
            )
            
            return (filename, content)
            
        except Exception as e:
            logger.error(f"Error processing point {point_id} ({lat}, {lon}): {e}")
            return None


class ZipFileBuilder:
    """Build zip files from ICASA weather files"""
    
    @staticmethod
    def create_zip_archive(
        files: Dict[str, str],
        archive_name: str = "weather_data.zip",
        include_readme: bool = True,
        metadata: Optional[Dict] = None,
        shapefile_path: Optional[str] = None
    ) -> bytes:
        """
        Create a zip archive from ICASA files.
        
        Args:
            files: Dictionary mapping filenames to file contents
            archive_name: Name for the zip archive
            include_readme: Include a README file with metadata
            metadata: Optional metadata to include in README
            shapefile_path: Optional path to shapefile to include in archive
            
        Returns:
            Zip file content as bytes
        """
        from pathlib import Path
        
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Add each ICASA file
            for filename, content in files.items():
                zip_file.writestr(filename, content)
            
            # Add shapefile if provided
            if shapefile_path:
                shapefile_path = Path(shapefile_path)
                if shapefile_path.exists():
                    # Add the main .shp file
                    zip_file.write(
                        shapefile_path,
                        arcname=f"shapefile/{shapefile_path.name}"
                    )
                    
                    # Add companion files (.shx, .dbf, .prj, etc.)
                    base_path = shapefile_path.with_suffix('')
                    companion_extensions = ['.shx', '.dbf', '.prj', '.cpg', '.qpj']
                    
                    for ext in companion_extensions:
                        companion_file = base_path.with_suffix(ext)
                        if companion_file.exists():
                            zip_file.write(
                                companion_file,
                                arcname=f"shapefile/{companion_file.name}"
                            )
            
            # Add README if requested
            if include_readme:
                readme_content = ZipFileBuilder._generate_readme(
                    file_count=len(files),
                    metadata=metadata,
                    has_shapefile=shapefile_path is not None
                )
                zip_file.writestr('README.txt', readme_content)
        
        zip_buffer.seek(0)
        return zip_buffer.getvalue()
    
    @staticmethod
    def _generate_readme(file_count: int, metadata: Optional[Dict] = None, has_shapefile: bool = False) -> str:
        """Generate README content for zip archive."""
        readme = io.StringIO()
        
        readme.write("=" * 70 + "\n")
        readme.write("ICASA Weather Data Package\n")
        readme.write("=" * 70 + "\n\n")
        
        readme.write(f"Total ICASA Files: {file_count}\n")
        if has_shapefile:
            readme.write("Shapefile: Included in 'shapefile/' directory\n")
        readme.write("\n")
        
        if metadata:
            readme.write("Package Information:\n")
            readme.write("-" * 70 + "\n")
            if 'start_date' in metadata:
                readme.write(f"Date Range: {metadata['start_date']} to {metadata['end_date']}\n")
            if 'total_points' in metadata:
                readme.write(f"Total Coordinates: {metadata['total_points']}\n")
            if 'variables' in metadata:
                readme.write(f"Variables: {', '.join(metadata['variables'])}\n")
            if 'data_source' in metadata:
                readme.write(f"Data Source: {metadata['data_source']}\n")
            readme.write("\n")
        
        readme.write("File Format: ICASA Weather Format\n")
        readme.write("-" * 70 + "\n")
        readme.write("Each file contains daily weather data for a specific coordinate.\n")
        readme.write("Filename format: weather_pointXXXX_LAT_LON_STARTDATE_ENDDATE.txt\n")
        readme.write("(or point_id.txt if point IDs were available in the shapefile)\n\n")
        
        readme.write("File Structure:\n")
        readme.write("  - Header with site information\n")
        readme.write("  - Location coordinates (latitude, longitude)\n")
        readme.write("  - Daily weather data in YYYYDDD format\n\n")
        
        readme.write("Variables:\n")
        readme.write("  - RAIN: Precipitation (mm/day)\n")
        readme.write("  (Additional variables may be added in future versions)\n\n")
        
        if has_shapefile:
            readme.write("Shapefile Information:\n")
            readme.write("-" * 70 + "\n")
            readme.write("The original shapefile with point definitions is included in the\n")
            readme.write("'shapefile/' subdirectory. This includes all companion files (.shx,\n")
            readme.write(".dbf, etc.) needed to open the shapefile in GIS software.\n\n")
        
        readme.write("Usage:\n")
        readme.write("  These files are compatible with DSSAT and other crop models\n")
        readme.write("  that support the ICASA weather format.\n\n")
        
        readme.write("=" * 70 + "\n")
        
        return readme.getvalue()


# Convenience function for direct use
async def generate_weather_package(
    dataset: xr.Dataset,
    coordinates: List[Tuple[float, float]],
    start_date: str,
    end_date: str,
    variables: Optional[List[str]] = None,
    max_workers: Optional[int] = None,
    batch_size: int = 50
) -> bytes:
    """
    Generate a complete weather data package as a zip file.
    
    Args:
        dataset: Xarray dataset
        coordinates: List of (lon, lat) tuples
        start_date: Start date
        end_date: End date
        variables: Variables to include
        max_workers: Max worker threads/processes
        batch_size: Batch size for processing
        
    Returns:
        Zip file content as bytes
    """
    # Process coordinates
    processor = AsyncMultiPointProcessor(dataset, max_workers=max_workers)
    files = await processor.process_coordinates_batch(
        coordinates=coordinates,
        start_date=start_date,
        end_date=end_date,
        variables=variables,
        batch_size=batch_size
    )
    
    # Create zip archive
    metadata = {
        'start_date': start_date,
        'end_date': end_date,
        'total_points': len(coordinates),
        'variables': variables or ['precipitation'],
        'data_source': 'CHIRPS'
    }
    
    zip_content = ZipFileBuilder.create_zip_archive(
        files=files,
        include_readme=True,
        metadata=metadata
    )
    
    return zip_content
