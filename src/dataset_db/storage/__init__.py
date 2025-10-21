"""
Storage layer for Parquet files and indexes.

Handles writing partitioned Parquet files and managing storage layout.
"""

from .parquet_writer import ParquetWriter
from .layout import StorageLayout

__all__ = ["ParquetWriter", "StorageLayout"]
