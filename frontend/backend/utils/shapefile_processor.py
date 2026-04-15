"""
Shapefile Processing Utilities
Extracts coordinates from shapefile for multi-point weather data generation
"""
import logging
import os
import math
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

from .point_id import generate_point_id

logger = logging.getLogger(__name__)


class ShapefileProcessor:
    """Process shapefiles and extract coordinates"""

    @staticmethod
    def calculate_bounds(coordinates: List[Tuple[float, float]]) -> Dict[str, float]:
        """Calculate bounding coordinates for a collection of lon/lat points."""
        if not coordinates:
            raise ValueError("Cannot calculate bounds for an empty coordinate list")

        longitudes = [lon for lon, _ in coordinates]
        latitudes = [lat for _, lat in coordinates]

        return {
            "lon_min": min(longitudes),
            "lon_max": max(longitudes),
            "lat_min": min(latitudes),
            "lat_max": max(latitudes),
        }
    
    @staticmethod
    def extract_coordinates_and_ids_from_file(
        file_path: Path
    ) -> Tuple[List[Tuple[float, float]], Dict[int, str], Dict[str, Any]]:
        """
        Extract coordinates and point IDs from a spatial file (shapefile or geojson).
        Supports shapefile (.shp), GeoJSON (.geojson, .json) and other formats supported by geopandas.
        
        Args:
            file_path: Path to the shapefile (.shp) or geojson (.geojson, .json) file
            
        Returns:
            Tuple of (coordinates_list, id_mapping, extraction_metadata)
            - coordinates_list: List of (longitude, latitude) tuples
            - id_mapping: Dict mapping index to point ID
            - extraction_metadata: Dict with source and generated-id details
        """
        if gpd is None:
            raise ImportError("geopandas is required. Install with: pip install geopandas shapely")
        
        try:
            # Read the file using geopandas (supports shapefiles, geojson, etc.)
            gdf = gpd.read_file(str(file_path))
            logger.info(f"Loaded {len(gdf)} features from {file_path}")
            
            coordinates = []
            id_mapping = {}  # Maps index to point ID
            generated_id_indices = []
            used_ids = set()
            coord_index = 0
            seen_coords = set()
            
            # Look for ID column - check common naming conventions
            id_column = None
            cols = [col.lower() for col in gdf.columns]
            # for col_name in ['id', 'ID', 'point_id', 'POINT_ID', 'pid', 'PID']:
            #     if col_name in gdf.columns:
            #         id_column = col_name
            #         logger.info(f"Found ID column: {id_column}")
            #         break

            for col_name in ['id', 'point_id', 'pid', 'cell_id', 'id_column', 'cellid', 'pointid']:
                if col_name in cols:
                    id_column = gdf.columns[cols.index(col_name)]
                    logger.info(f"Found ID column: {id_column}")
                    break

            # Extract coordinates and IDs from each feature
            for _, row in gdf.iterrows():
                geom = row.geometry
                # Use ID from column when available and non-empty; otherwise use hash fallback per coordinate.
                feature_id = None
                if id_column:
                    raw_feature_id = row[id_column]
                    is_missing = (
                        raw_feature_id is None
                        or (isinstance(raw_feature_id, float) and math.isnan(raw_feature_id))
                        or str(raw_feature_id).strip() == ''
                    )
                    if not is_missing:
                        feature_id = str(raw_feature_id)
                
                if geom.is_empty:
                    continue
                
                # Handle different geometry types
                feature_coords = []
                if geom.geom_type == 'Point':
                    feature_coords = [(geom.x, geom.y)]
                
                elif geom.geom_type == 'MultiPoint':
                    feature_coords = [(point.x, point.y) for point in geom.geoms]
                
                elif geom.geom_type in ['LineString', 'MultiLineString']:
                    if geom.geom_type == 'LineString':
                        feature_coords = [(coord[0], coord[1]) for coord in geom.coords]
                    else:
                        feature_coords = [(coord[0], coord[1]) for line in geom.geoms for coord in line.coords]
                
                elif geom.geom_type in ['Polygon', 'MultiPolygon']:
                    if geom.geom_type == 'Polygon':
                        feature_coords = [(coord[0], coord[1]) for coord in geom.exterior.coords]
                    else:
                        feature_coords = [(coord[0], coord[1]) for poly in geom.geoms for coord in poly.exterior.coords]
                
                # Add extracted coordinates with their ID mapping
                for coord in feature_coords:
                    rounded = (round(coord[0], 4), round(coord[1], 4))
                    # Deduplicate in O(1) using rounded coordinate keys.
                    if rounded not in seen_coords:
                        seen_coords.add(rounded)
                        point_id = feature_id
                        if point_id is None:
                            salt = 0
                            point_id = generate_point_id(lat=coord[1], lon=coord[0], length=8, salt=salt)
                            while point_id in used_ids:
                                salt += 1
                                point_id = generate_point_id(lat=coord[1], lon=coord[0], length=8, salt=salt)
                            generated_id_indices.append(coord_index)

                        used_ids.add(str(point_id))

                        id_mapping[coord_index] = point_id
                        coordinates.append(coord)
                        coord_index += 1
            
            logger.info(f"Extracted {len(coordinates)} coordinates with {len(set(id_mapping.values()))} unique point IDs")
            extraction_metadata = {
                "source_name": file_path.stem,
                "id_column": id_column,
                "generated_id_indices": generated_id_indices,
                "has_generated_ids": len(generated_id_indices) > 0,
            }
            return coordinates, id_mapping, extraction_metadata
            
        except Exception as e:
            logger.error(f"Error extracting coordinates from file: {e}")
            raise ValueError(f"Failed to process file: {str(e)}")
    
    @staticmethod
    def extract_coordinates_from_shapefile(
        shapefile_path: Path
    ) -> List[Tuple[float, float]]:
        """
        Extract coordinates from a shapefile (legacy method for backward compatibility).
        
        Args:
            shapefile_path: Path to the .shp file
            
        Returns:
            List of (longitude, latitude) tuples
        """
        # Use the new method and return only coordinates
        coordinates, _, _ = ShapefileProcessor.extract_coordinates_and_ids_from_file(shapefile_path)
        return coordinates
    
    @staticmethod
    def save_uploaded_shapefile(
        uploaded_file: bytes,
        filename: str,
        additional_files: Optional[Dict[str, bytes]] = None
    ) -> Path:
        """
        Save uploaded shapefile to temporary location.
        Supports single .shp file or multiple files (.shp, .shx, .dbf, .geojson, .json).
        
        Args:
            uploaded_file: File content as bytes (primary file)
            filename: Original filename of primary file
            additional_files: Optional dict mapping filenames to file contents for companion files
            
        Returns:
            Path to the saved spatial file (.shp or .geojson)
            
        Raises:
            ValueError: If file format is invalid or required companion files are missing
        """
        # Create temporary directory
        temp_dir = Path(tempfile.mkdtemp())
        
        try:
            # Determine file type
            filename_lower = filename.lower()
            
            # Handle zip file containing shapefile components
            if filename_lower.endswith('.zip'):
                zip_path = temp_dir / filename
                with open(zip_path, 'wb') as f:
                    f.write(uploaded_file)
                
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                # Find spatial file in extracted contents
                shp_files = list(temp_dir.glob('**/*.shp'))
                geojson_files = list(temp_dir.glob('**/*.geojson')) + list(temp_dir.glob('**/*.json'))
                
                if shp_files:
                    shp_path = shp_files[0]
                    # Verify companion files exist
                    base_path = shp_path.with_suffix('')
                    required_files = {
                        'shx': base_path.with_suffix('.shx'),
                        'dbf': base_path.with_suffix('.dbf')
                    }
                    
                    missing = [name for name, path in required_files.items() if not path.exists()]
                    if missing:
                        raise ValueError(
                            f"Incomplete shapefile in zip. Missing: {', '.join(missing).upper()}. "
                            f"Required files: .shp, .shx, .dbf"
                        )
                    return shp_path
                
                elif geojson_files:
                    return geojson_files[0]
                
                else:
                    raise ValueError("No spatial file (.shp or .geojson) found in zip archive")
            
            # Handle GeoJSON files
            elif filename_lower.endswith(('.geojson', '.json')):
                spatial_path = temp_dir / filename
                with open(spatial_path, 'wb') as f:
                    f.write(uploaded_file)
                return spatial_path
            
            # Handle shapefile .shp file
            elif filename_lower.endswith('.shp'):
                shp_path = temp_dir / filename
                with open(shp_path, 'wb') as f:
                    f.write(uploaded_file)
                
                # Save companion files if provided
                base_name = filename[:-4]  # Remove .shp extension
                required_companions = ['.shx', '.dbf']
                saved_companions = []
                
                if additional_files:
                    for ext in required_companions:
                        for provided_name, content in additional_files.items():
                            if provided_name.lower().endswith(ext):
                                companion_path = temp_dir / f"{base_name}{ext}"
                                with open(companion_path, 'wb') as f:
                                    f.write(content)
                                saved_companions.append(ext)
                                break
                
                # Check if all required companions are present
                missing = [ext for ext in required_companions if ext not in saved_companions]
                if missing:
                    raise ValueError(
                        f"Incomplete shapefile. Missing required files: {', '.join(missing)}. "
                        f"Please provide all three files: .shp, .shx, .dbf"
                    )
                
                return shp_path
            
            else:
                raise ValueError(
                    f"Unsupported file format: {filename}. "
                    f"Supported formats: .shp (with .shx, .dbf), .geojson, .json, .zip"
                )
            
        except Exception as e:
            # Clean up temp directory on error
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise ValueError(f"Failed to save spatial file: {str(e)}")
    
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
    def validate_coordinates_with_ids(
        coordinates: List[Tuple[float, float]],
        id_mapping: Dict[int, str],
        max_points: int = 1000,
        lat_bounds: Tuple[float, float] = (-90, 90),
        lon_bounds: Tuple[float, float] = (-180, 180)
    ) -> Dict[str, Any]:
        """
        Validate extracted coordinates with IDs.
        
        Args:
            coordinates: List of (lon, lat) tuples
            id_mapping: Dict mapping index to point ID
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
                "message": "No coordinates found in file",
                "issues": ["Empty file"]
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
            point_id = id_mapping.get(i, str(i+1))
            if not (lat_bounds[0] <= lat <= lat_bounds[1]):
                issues.append(f"Point {point_id}: Latitude {lat} out of bounds")
            elif not (lon_bounds[0] <= lon <= lon_bounds[1]):
                issues.append(f"Point {point_id}: Longitude {lon} out of bounds")
            else:
                valid_coordinates.append((lon, lat))
        
        if len(valid_coordinates) == 0:
            return {
                "valid": False,
                "message": "No valid coordinates found",
                "issues": issues[:10]
            }
        
        return {
            "valid": True,
            "message": f"Found {len(valid_coordinates)} valid coordinates",
            "total_points": len(coordinates),
            "valid_points": len(valid_coordinates),
            "invalid_points": len(coordinates) - len(valid_coordinates),
            "issues": issues[:5] if issues else []
        }
    
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
