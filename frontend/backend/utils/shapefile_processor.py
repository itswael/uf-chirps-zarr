"""
Shapefile Processing Utilities
Extracts coordinates from shapefile for multi-point weather data generation
"""
import logging
import os
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
import tempfile
import zipfile
import shutil

# Set environment variable to allow geopandas to restore missing .shx files
os.environ["SHAPE_RESTORE_SHX"] = "YES"

try:
    import geopandas as gpd
except ImportError:
    gpd = None

logger = logging.getLogger(__name__)


class ShapefileProcessor:
    """Process shapefiles and extract coordinates"""
    
    @staticmethod
    def extract_coordinates_from_shapefile(
        shapefile_path: Path
    ) -> List[Tuple[float, float]]:
        """
        Extract coordinates from a shapefile.
        
        Args:
            shapefile_path: Path to the .shp file
            
        Returns:
            List of (longitude, latitude) tuples
        """
        if gpd is None:
            raise ImportError("geopandas is required. Install with: pip install geopandas shapely")
        
        try:
            # Read the shapefile using geopandas
            # This will work even if .shx and .dbf are missing thanks to SHAPE_RESTORE_SHX
            gdf = gpd.read_file(str(shapefile_path))
            
            coordinates = []
            
            # Extract coordinates based on geometry type
            for geom in gdf.geometry:
                if geom.is_empty:
                    continue
                    
                # Handle different geometry types
                if geom.geom_type == 'Point':
                    coordinates.append((geom.x, geom.y))
                
                elif geom.geom_type == 'MultiPoint':
                    for point in geom.geoms:
                        coordinates.append((point.x, point.y))
                
                elif geom.geom_type in ['LineString', 'MultiLineString']:
                    # Extract all points from line
                    if geom.geom_type == 'LineString':
                        coords = list(geom.coords)
                    else:
                        coords = [coord for line in geom.geoms for coord in line.coords]
                    coordinates.extend(coords)
                
                elif geom.geom_type in ['Polygon', 'MultiPolygon']:
                    # Extract all vertices from polygon
                    if geom.geom_type == 'Polygon':
                        coords = list(geom.exterior.coords)
                    else:
                        coords = [coord for poly in geom.geoms for coord in poly.exterior.coords]
                    coordinates.extend(coords)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_coordinates = []
            for coord in coordinates:
                # Round to 4 decimal places to identify near-duplicates
                rounded = (round(coord[0], 4), round(coord[1], 4))
                if rounded not in seen:
                    seen.add(rounded)
                    unique_coordinates.append(coord)
            
            logger.info(f"Extracted {len(unique_coordinates)} unique coordinates from shapefile")
            return unique_coordinates
            
        except Exception as e:
            logger.error(f"Error extracting coordinates from shapefile: {e}")
            raise ValueError(f"Failed to process shapefile: {str(e)}")
    
    @staticmethod
    def save_uploaded_shapefile(
        uploaded_file: bytes,
        filename: str
    ) -> Path:
        """
        Save uploaded shapefile to temporary location.
        
        Args:
            uploaded_file: File content as bytes
            filename: Original filename
            
        Returns:
            Path to the saved .shp file
        """
        # Create temporary directory
        temp_dir = Path(tempfile.mkdtemp())
        
        try:
            # Save the uploaded file
            shp_path = temp_dir / filename
            with open(shp_path, 'wb') as f:
                f.write(uploaded_file)
            
            return shp_path
            
        except Exception as e:
            # Clean up temp directory on error
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise ValueError(f"Failed to save shapefile: {str(e)}")
    
    @staticmethod
    def extract_shapefile_from_upload(
        uploaded_file: bytes,
        filename: str
    ) -> Path:
        """
        Extract shapefile from uploaded zip file.
        
        Args:
            uploaded_file: File content as bytes
            filename: Original filename
            
        Returns:
            Path to the extracted .shp file
        """
        # Create temporary directory
        temp_dir = Path(tempfile.mkdtemp())
        
        try:
            if filename.endswith('.zip'):
                # Extract zip file
                zip_path = temp_dir / filename
                with open(zip_path, 'wb') as f:
                    f.write(uploaded_file)
                
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                # Find .shp file in extracted contents
                shp_files = list(temp_dir.glob('**/*.shp'))
                if not shp_files:
                    raise ValueError("No .shp file found in zip archive")
                
                shp_path = shp_files[0]
                
                # Check for required companion files
                base_path = shp_path.with_suffix('')
                shx_path = base_path.with_suffix('.shx')
                dbf_path = base_path.with_suffix('.dbf')
                
                missing_files = []
                if not shx_path.exists():
                    # Check case-insensitive
                    shx_upper = base_path.with_suffix('.SHX')
                    if not shx_upper.exists():
                        missing_files.append('.shx')
                
                if not dbf_path.exists():
                    # Check case-insensitive
                    dbf_upper = base_path.with_suffix('.DBF')
                    if not dbf_upper.exists():
                        missing_files.append('.dbf')
                
                if missing_files:
                    raise ValueError(
                        f"Incomplete shapefile. Missing required files: {', '.join(missing_files)}. "
                        f"Please include all shapefile components (.shp, .shx, .dbf) in the zip."
                    )
                
                return shp_path
            
            else:
                raise ValueError(
                    "Please upload shapefile components (.shp, .shx, .dbf)"
                )
                
        except Exception as e:
            # Clean up temp directory on error
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise
    
    @staticmethod
    def validate_coordinates(
        coordinates: List[Tuple[float, float]],
        max_points: int = 1000,
        lat_bounds: Tuple[float, float] = (-90, 90),
        lon_bounds: Tuple[float, float] = (-180, 180)
    ) -> Dict[str, Any]:
        """
        Validate extracted coordinates.
        
        Args:
            coordinates: List of (lon, lat) tuples
            max_points: Maximum allowed points
            lat_bounds: Valid latitude range
            lon_bounds: Valid longitude range
            
        Returns:
            Validation result dict with status and messages
        """
        issues = []
        
        # Check point count
        if len(coordinates) == 0:
            return {
                "valid": False,
                "message": "No coordinates found in shapefile",
                "issues": ["Empty shapefile"]
            }
        
        if len(coordinates) > max_points:
            return {
                "valid": False,
                "message": f"Too many points: {len(coordinates)} (max: {max_points})",
                "issues": [f"Exceeds maximum point limit of {max_points}"]
            }
        
        # Check coordinate validity
        valid_coordinates = []
        for i, (lon, lat) in enumerate(coordinates):
            if not (lat_bounds[0] <= lat <= lat_bounds[1]):
                issues.append(f"Point {i+1}: Latitude {lat} out of bounds")
            elif not (lon_bounds[0] <= lon <= lon_bounds[1]):
                issues.append(f"Point {i+1}: Longitude {lon} out of bounds")
            else:
                valid_coordinates.append((lon, lat))
        
        if len(valid_coordinates) == 0:
            return {
                "valid": False,
                "message": "No valid coordinates found",
                "issues": issues[:10]  # Limit to first 10 issues
            }
        
        return {
            "valid": True,
            "message": f"Found {len(valid_coordinates)} valid coordinates",
            "total_points": len(coordinates),
            "valid_points": len(valid_coordinates),
            "invalid_points": len(coordinates) - len(valid_coordinates),
            "issues": issues[:5] if issues else []
        }
