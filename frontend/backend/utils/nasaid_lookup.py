"""
NASA ID fallback datastore.

Design:
- Build once from legacy NASAID shapefile into SQLite datastore.
- Runtime lookups read from SQLite only (no shapefile required).
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path
from typing import Optional, Tuple

try:
    import geopandas as gpd
except ImportError:
    gpd = None

logger = logging.getLogger(__name__)

GRID_RESOLUTION = 0.005
DATASTORE_VERSION = "0.005"
MIN_LAT = -90.0
MAX_LAT = 90.0
MIN_LON = -180.0
MAX_LON = 180.0

BASE_DIR = Path(__file__).resolve().parents[1]
DATASTORE_PATH = BASE_DIR / "data" / "nasaid_lookup.sqlite"
LEGACY_SOURCE_PATH = BASE_DIR.parent / "sample_data" / "nasaid" / "five_arc_land2_nasa.shp"


class NasaIdDatastore:
    """Thread-safe SQLite-backed lookup for 0.005-degree NASA IDs."""

    def __init__(self, db_path: Path = DATASTORE_PATH):
        self.db_path = db_path
        self._thread_local = threading.local()

    @staticmethod
    def _clamp(value: float, minimum: float, maximum: float) -> float:
        if value < minimum:
            return minimum
        if value > maximum:
            return maximum
        return value

    @classmethod
    def _quantize_index(cls, value: float, minimum: float) -> int:
        value = cls._clamp(value, minimum, MAX_LAT if minimum == MIN_LAT else MAX_LON)
        return int((value - minimum) / GRID_RESOLUTION)

    @classmethod
    def _coord_key(cls, lon: float, lat: float) -> Tuple[int, int]:
        lat_idx = cls._quantize_index(lat, MIN_LAT)
        lon_idx = cls._quantize_index(lon, MIN_LON)
        return lat_idx, lon_idx

    def _get_connection(self) -> Optional[sqlite3.Connection]:
        if not self.db_path.exists():
            return None

        conn = getattr(self._thread_local, "conn", None)
        if conn is None:
            uri = f"file:{self.db_path.as_posix()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
            conn.execute("PRAGMA query_only=ON")
            conn.execute("PRAGMA temp_store=MEMORY")
            self._thread_local.conn = conn
        return conn

    def is_compatible(self) -> bool:
        """Check whether the on-disk datastore matches the current grid resolution."""
        conn = self._get_connection()
        if conn is None:
            return False

        try:
            row = conn.execute(
                "SELECT value FROM nasaid_metadata WHERE key = 'grid_resolution'"
            ).fetchone()
            return row is not None and str(row[0]) == DATASTORE_VERSION
        except sqlite3.Error:
            return False

    def lookup(self, lon: float, lat: float) -> Optional[str]:
        conn = self._get_connection()
        if conn is None:
            return None

        lat_idx, lon_idx = self._coord_key(lon, lat)
        row = conn.execute(
            "SELECT nasaid FROM nasaid_grid WHERE lat_idx = ? AND lon_idx = ?",
            (lat_idx, lon_idx),
        ).fetchone()
        if row is not None:
            return str(row[0])

        # No exact datastore hit: compute a deterministic 0.005-degree NASAID
        # directly from the coordinate so adjacent cells do not collapse to a
        # single nearest stored name.
        lon_slots = int((MAX_LON - MIN_LON) / GRID_RESOLUTION) + 1
        return str(lat_idx * lon_slots + lon_idx + 1)


def _resolve_columns(gdf) -> Tuple[str, str, str]:
    cols_lower = {col.lower(): col for col in gdf.columns}

    id_col = None
    for candidate in ["nasapid", "nasaid", "id", "point_id", "pid", "cell_id"]:
        if candidate in cols_lower:
            id_col = cols_lower[candidate]
            break

    if id_col is None:
        raise ValueError("No NASA ID column found in source data")

    if "lonnp" in cols_lower and "latnp" in cols_lower:
        return id_col, cols_lower["lonnp"], cols_lower["latnp"]

    if "longitude" in cols_lower and "latitude" in cols_lower:
        return id_col, cols_lower["longitude"], cols_lower["latitude"]

    return id_col, "__geom_lon__", "__geom_lat__"


def build_nasaid_datastore(
    source_path: Path = LEGACY_SOURCE_PATH,
    db_path: Path = DATASTORE_PATH,
    force_rebuild: bool = False,
) -> Path:
    """
    Build SQLite datastore once from legacy NASAID source shapefile.

    Returns the datastore path.
    """
    if db_path.exists() and not force_rebuild:
        return db_path

    if gpd is None:
        raise RuntimeError("geopandas is required to build NASA ID datastore")

    if not source_path.exists():
        raise FileNotFoundError(f"NASAID source file not found: {source_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists() and force_rebuild:
        db_path.unlink()

    logger.info("Building NASA ID datastore from %s", source_path)
    gdf = gpd.read_file(str(source_path))
    id_col, lon_col, lat_col = _resolve_columns(gdf)

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("DROP TABLE IF EXISTS nasaid_metadata")
        conn.execute("DROP TABLE IF EXISTS nasaid_grid")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nasaid_grid (
                lat_idx INTEGER NOT NULL,
                lon_idx INTEGER NOT NULL,
                nasaid TEXT NOT NULL,
                PRIMARY KEY (lat_idx, lon_idx)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nasaid_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

        def coord_key(lon: float, lat: float) -> Tuple[int, int]:
            lat_clamped = max(MIN_LAT, min(MAX_LAT, float(lat)))
            lon_clamped = max(MIN_LON, min(MAX_LON, float(lon)))
            lat_idx = int((lat_clamped - MIN_LAT) / GRID_RESOLUTION)
            lon_idx = int((lon_clamped - MIN_LON) / GRID_RESOLUTION)
            return lat_idx, lon_idx

        rows = []
        for _, row in gdf.iterrows():
            if lon_col == "__geom_lon__":
                geom = row.geometry
                if geom is None or geom.is_empty:
                    continue
                lon = float(geom.x)
                lat = float(geom.y)
            else:
                lon = float(row[lon_col])
                lat = float(row[lat_col])

            lat_idx, lon_idx = coord_key(lon, lat)
            lon_slots = int((MAX_LON - MIN_LON) / GRID_RESOLUTION) + 1
            generated_id = lat_idx * lon_slots + lon_idx + 1
            rows.append((lat_idx, lon_idx, str(generated_id)))

        conn.executemany(
            "INSERT OR REPLACE INTO nasaid_grid (lat_idx, lon_idx, nasaid) VALUES (?, ?, ?)",
            rows,
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_nasaid_grid ON nasaid_grid(lat_idx, lon_idx)")
        conn.execute(
            "INSERT OR REPLACE INTO nasaid_metadata (key, value) VALUES ('grid_resolution', ?)",
            (DATASTORE_VERSION,),
        )
        conn.commit()

        logger.info("NASA ID datastore built at %s with %d records", db_path, len(rows))
    finally:
        conn.close()

    return db_path


_DATASTORE = NasaIdDatastore()


def ensure_nasaid_datastore() -> Optional[Path]:
    """Ensure datastore exists; build it once from legacy source if available."""
    if DATASTORE_PATH.exists() and _DATASTORE.is_compatible():
        return DATASTORE_PATH

    if DATASTORE_PATH.exists() and not _DATASTORE.is_compatible():
        try:
            DATASTORE_PATH.unlink()
        except Exception:
            pass

    if LEGACY_SOURCE_PATH.exists():
        try:
            return build_nasaid_datastore()
        except Exception as exc:
            logger.error("Failed to build NASA ID datastore: %s", exc)
            return None

    logger.warning("NASA ID datastore is missing and legacy source file is unavailable")
    return None


def get_nasaid(lon: float, lat: float) -> Optional[str]:
    """
    Lookup fallback NASA ID from SQLite datastore.

    If datastore lookup is unavailable, fall back to a deterministic
    0.005-degree global grid ID so filename generation never reverts
    to the legacy coordinate-based naming.
    """
    ensure_nasaid_datastore()
    nasaid = _DATASTORE.lookup(lon=lon, lat=lat)
    if nasaid is not None:
        return nasaid

    # Deterministic backup ID in global 0.005-degree index space.
    lat_clamped = max(MIN_LAT, min(MAX_LAT, float(lat)))
    lon_clamped = max(MIN_LON, min(MAX_LON, float(lon)))
    lat_idx = int((lat_clamped - MIN_LAT) / GRID_RESOLUTION)
    lon_idx = int((lon_clamped - MIN_LON) / GRID_RESOLUTION)
    lon_slots = int((MAX_LON - MIN_LON) / GRID_RESOLUTION) + 1
    fallback_id = lat_idx * lon_slots + lon_idx + 1
    return str(fallback_id)
