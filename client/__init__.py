"""
CHIRPS Zarr Client Package.

A client library for accessing CHIRPS v3.0 Zarr data with support for
various access patterns, concurrent operations, and performance testing.
"""
from .config import ClientConfig, config
from .zarr_client import ChirpsZarrClient

__all__ = ['ChirpsZarrClient', 'ClientConfig', 'config']
__version__ = '1.0.0'
