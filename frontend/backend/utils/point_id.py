"""
Deterministic point ID utilities.

Generates short, stable alphanumeric IDs from latitude/longitude pairs.
"""

from __future__ import annotations

import base64
import hashlib


def generate_point_id(lat: float, lon: float, length: int = 8, salt: int = 0) -> str:
    """Return a stable uppercase alphanumeric ID derived from lat/lon."""
    if length < 4:
        raise ValueError("length must be at least 4")

    # Include explicit labels and signs so (lat, lon) is never confused with (lon, lat)
    # and (-10, 10) differs from (10, -10).
    payload = f"lat={float(lat):+.8f};lon={float(lon):+.8f};salt={salt}".encode("ascii")
    digest = hashlib.blake2b(payload, digest_size=10).digest()
    token = base64.b32encode(digest).decode("ascii").rstrip("=")
    return token[:length]
