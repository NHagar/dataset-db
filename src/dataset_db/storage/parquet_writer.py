"""
Parquet writer with partitioned storage and optimized encoding.

Implements spec.md §2.1 Parquet storage with:
- Partitioning by dataset_id and domain_prefix
- ZSTD compression with dictionary encoding
- Row group size management
- Partition-level buffering for efficient writes at scale
- Batch writing with automatic partition handling
"""

from pathlib import Path
from typing import Optional

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from dataset_db.config import get_config
from dataset_db.storage.layout import StorageLayout


class ParquetWriter:
    """
    Write normalized URL data to partitioned Parquet files.

    Implements the storage layout and encoding from spec.md §2.1:
    - Partitioned by dataset_id and domain_prefix
    - ZSTD compression (level 6-9)
    - Dictionary encoding for scheme, host, path_query, domain
    - Target row group size: 128MB (configurable)
    - Partition-level buffering for efficient writes at scale
    """

    def __init__(
        self,
        base_path: Optional[Path] = None,
        compression: Optional[str] = None,
        compression_level: Optional[int] = None,
        row_group_size: Optional[int] = None,
        partition_buffer_size: Optional[int] = None,
    ):
        """
        Initialize Parquet writer.

        Args:
            base_path: Root directory for storage (defaults to config)
            compression: Compression codec (defaults to config, typically 'zstd')
            compression_level: Compression level (defaults to config, typically 6)
            row_group_size: Target row group size in bytes (defaults to 128MB)
            partition_buffer_size: Buffer size per partition before flushing (defaults to 128MB)
        """
        self.config = get_config()

        # Use provided values or fall back to config
        self.base_path = base_path or self.config.storage.base_path
        self.compression = (
            compression or self.config.ingestion.compression
        )
        self.compression_level = (
            compression_level or self.config.ingestion.compression_level
        )
        self.row_group_size = (
            row_group_size or self.config.ingestion.row_group_size
        )
        self.partition_buffer_size = (
            partition_buffer_size
            if partition_buffer_size is not None
            else self.config.ingestion.partition_buffer_size
        )

        # Initialize storage layout manager
        self.layout = StorageLayout(self.base_path)

        # Partition buffers: {(dataset_id, domain_prefix): [DataFrames]}
        self._partition_buffers: dict[tuple[int, str], list[pl.DataFrame]] = {}
        self._partition_buffer_sizes: dict[tuple[int, str], int] = {}

        # Statistics tracking
        self._stats = {
            "batches_written": 0,
            "rows_written": 0,
            "bytes_written": 0,
            "files_created": 0,
        }

    def write_batch(
        self,
        df: pl.DataFrame,
        dataset_id: Optional[int] = None,
        auto_flush: bool = True,
    ) -> dict[str, int]:
        """
        Write a batch of normalized URLs to partitioned Parquet files.

        The input DataFrame must have the schema from IngestionProcessor:
        - dataset_id: Int32
        - domain_id: Int64
        - url_id: Int64
        - scheme: String
        - host: String
        - path_query: String
        - domain: String
        - domain_prefix: String

        Data is buffered per partition and flushed when buffer size exceeds
        partition_buffer_size (default 128MB) or when auto_flush is True.

        Args:
            df: Normalized Polars DataFrame from IngestionProcessor
            dataset_id: Optional dataset ID filter (if df contains multiple datasets)
            auto_flush: Automatically flush full buffers (default True)

        Returns:
            Dictionary with write statistics (only includes flushed data)

        Raises:
            ValueError: If DataFrame has incorrect schema
        """
        # Validate schema
        self._validate_schema(df)

        if df.height == 0:
            return {"files_written": 0, "rows_written": 0}

        # Filter by dataset_id if specified
        if dataset_id is not None:
            df = df.filter(pl.col("dataset_id") == dataset_id)

        # Group by partition keys (dataset_id, domain_prefix)
        partitions = df.partition_by(
            ["dataset_id", "domain_prefix"], maintain_order=True
        )

        files_written = 0
        rows_written = 0

        for partition_df in partitions:
            # Extract partition values from first row
            first_row = partition_df.row(0, named=True)
            ds_id = first_row["dataset_id"]
            domain_prefix = first_row["domain_prefix"]

            # Remove partition columns before buffering (they're in the directory path)
            write_df = partition_df.select([
                "domain_id",
                "url_id",
                "scheme",
                "host",
                "path_query",
                "domain",
            ])

            # Add to partition buffer
            partition_key = (ds_id, domain_prefix)
            if partition_key not in self._partition_buffers:
                self._partition_buffers[partition_key] = []
                self._partition_buffer_sizes[partition_key] = 0

            self._partition_buffers[partition_key].append(write_df)

            # Estimate buffer size (approximate uncompressed bytes)
            buffer_size = write_df.to_arrow().nbytes
            self._partition_buffer_sizes[partition_key] += buffer_size

            # Flush if buffer exceeds threshold and auto_flush is enabled
            # partition_buffer_size=0 means immediate writes (no buffering)
            if auto_flush and (
                self.partition_buffer_size == 0
                or self._partition_buffer_sizes[partition_key] >= self.partition_buffer_size
            ):
                flush_result = self._flush_partition(ds_id, domain_prefix)
                files_written += flush_result["files_written"]
                rows_written += flush_result["rows_written"]

        # Update stats
        self._stats["batches_written"] += 1

        return {
            "files_written": files_written,
            "rows_written": rows_written,
        }

    def flush(self, dataset_id: Optional[int] = None, domain_prefix: Optional[str] = None) -> dict[str, int]:
        """
        Flush buffered data to disk.

        Args:
            dataset_id: Optional dataset ID to flush (if None, flush all)
            domain_prefix: Optional domain prefix to flush (requires dataset_id)

        Returns:
            Dictionary with write statistics
        """
        files_written = 0
        rows_written = 0

        if dataset_id is not None and domain_prefix is not None:
            # Flush specific partition
            result = self._flush_partition(dataset_id, domain_prefix)
            files_written += result["files_written"]
            rows_written += result["rows_written"]
        elif dataset_id is not None:
            # Flush all partitions for a dataset
            keys_to_flush = [
                key for key in self._partition_buffers.keys()
                if key[0] == dataset_id
            ]
            for ds_id, dp in keys_to_flush:
                result = self._flush_partition(ds_id, dp)
                files_written += result["files_written"]
                rows_written += result["rows_written"]
        else:
            # Flush all partitions
            keys_to_flush = list(self._partition_buffers.keys())
            for ds_id, dp in keys_to_flush:
                result = self._flush_partition(ds_id, dp)
                files_written += result["files_written"]
                rows_written += result["rows_written"]

        return {
            "files_written": files_written,
            "rows_written": rows_written,
        }

    def _flush_partition(
        self,
        dataset_id: int,
        domain_prefix: str,
    ) -> dict[str, int]:
        """
        Flush a specific partition buffer to disk.

        Args:
            dataset_id: Dataset identifier
            domain_prefix: Domain prefix for partitioning

        Returns:
            Dictionary with write statistics
        """
        partition_key = (dataset_id, domain_prefix)

        if partition_key not in self._partition_buffers or not self._partition_buffers[partition_key]:
            return {"files_written": 0, "rows_written": 0}

        # Concatenate all buffered DataFrames for this partition
        buffered_dfs = self._partition_buffers[partition_key]
        combined_df = pl.concat(buffered_dfs)

        # Write to disk
        files_written = self._write_partition(combined_df, dataset_id, domain_prefix)
        rows_written = combined_df.height

        # Clear buffer
        del self._partition_buffers[partition_key]
        del self._partition_buffer_sizes[partition_key]

        # Update stats
        self._stats["rows_written"] += rows_written
        self._stats["files_created"] += files_written

        return {
            "files_written": files_written,
            "rows_written": rows_written,
        }

    def _write_partition(
        self,
        df: pl.DataFrame,
        dataset_id: int,
        domain_prefix: str,
    ) -> int:
        """
        Write a single partition to disk.

        Args:
            df: DataFrame with partition data (without partition columns)
            dataset_id: Dataset identifier
            domain_prefix: Domain prefix for partitioning

        Returns:
            Number of files created
        """
        # Ensure partition directory exists
        self.layout.ensure_partition_exists(dataset_id, domain_prefix)

        # Get next part number
        part_number = self.layout.get_next_part_number(
            dataset_id, domain_prefix
        )

        # Get output path
        output_path = self.layout.get_parquet_path(
            dataset_id, domain_prefix, part_number
        )

        # Convert Polars to PyArrow for fine-grained Parquet control
        arrow_table = df.to_arrow()

        # Build schema with dictionary encoding for string columns
        schema = self._build_schema_with_encoding(arrow_table.schema)

        # Cast table to use dictionary encoding
        arrow_table = arrow_table.cast(schema)

        # Write Parquet with optimized settings
        pq.write_table(
            arrow_table,
            output_path,
            compression=self.compression,
            compression_level=self.compression_level,
            row_group_size=self._estimate_row_group_rows(df),
            use_dictionary=True,
            write_statistics=True,
            version="2.6",  # Latest stable Parquet format
        )

        # Update byte count
        if output_path.exists():
            self._stats["bytes_written"] += output_path.stat().st_size

        return 1

    def _build_schema_with_encoding(
        self, original_schema: pa.Schema
    ) -> pa.Schema:
        """
        Build PyArrow schema with dictionary encoding for string columns.

        Per spec.md §2.1, these columns should use DICTIONARY encoding:
        - scheme
        - host
        - path_query
        - domain

        Args:
            original_schema: Original PyArrow schema

        Returns:
            Schema with dictionary encoding applied
        """
        dictionary_columns = {"scheme", "host", "path_query", "domain"}

        fields = []
        for field in original_schema:
            if field.name in dictionary_columns and pa.types.is_string(
                field.type
            ):
                # Convert to dictionary-encoded string
                fields.append(
                    pa.field(
                        field.name,
                        pa.dictionary(pa.int32(), pa.string()),
                        nullable=field.nullable,
                    )
                )
            else:
                fields.append(field)

        return pa.schema(fields)

    def _estimate_row_group_rows(self, df: pl.DataFrame) -> int:
        """
        Estimate number of rows per row group based on target size.

        Uses a heuristic: sample the DataFrame and estimate bytes per row,
        then calculate how many rows would fit in target row group size.

        Args:
            df: Input DataFrame

        Returns:
            Estimated number of rows per row group
        """
        if df.height == 0:
            return 1000  # Default minimum

        # Sample size for estimation (use smaller of 1000 or full size)
        sample_size = min(1000, df.height)
        sample_df = df.head(sample_size)

        # Convert to Arrow and estimate bytes
        arrow_table = sample_df.to_arrow()

        # Estimate uncompressed size
        # This is approximate - actual compressed size will be much smaller
        estimated_bytes = arrow_table.nbytes

        bytes_per_row = estimated_bytes / sample_size

        # Target row group size / bytes per row
        # Add 20% buffer since compression will reduce actual size
        rows_per_group = int(self.row_group_size / bytes_per_row * 1.2)

        # Clamp between reasonable bounds
        return max(1000, min(1_000_000, rows_per_group))

    def _validate_schema(self, df: pl.DataFrame) -> None:
        """
        Validate that DataFrame has the expected schema.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If schema is invalid
        """
        required_columns = {
            "dataset_id",
            "domain_id",
            "url_id",
            "scheme",
            "host",
            "path_query",
            "domain",
            "domain_prefix",
        }

        actual_columns = set(df.columns)

        if not required_columns.issubset(actual_columns):
            missing = required_columns - actual_columns
            raise ValueError(f"DataFrame missing required columns: {missing}")

        # Check types
        expected_types = {
            "dataset_id": pl.Int32,
            "domain_id": pl.Int64,
            "url_id": pl.Int64,
            "scheme": pl.Utf8,
            "host": pl.Utf8,
            "path_query": pl.Utf8,
            "domain": pl.Utf8,
            "domain_prefix": pl.Utf8,
        }

        for col, expected_type in expected_types.items():
            actual_type = df[col].dtype
            if actual_type != expected_type:
                raise ValueError(
                    f"Column '{col}' has type {actual_type}, "
                    f"expected {expected_type}"
                )

    def get_stats(self) -> dict:
        """
        Get writer statistics.

        Returns:
            Dictionary with statistics about writes performed
        """
        return {**self._stats}

    def get_storage_stats(self) -> dict:
        """
        Get storage layout statistics.

        Returns:
            Dictionary with storage statistics from StorageLayout
        """
        return self.layout.get_stats()

    def read_partition(
        self,
        dataset_id: int,
        domain_prefix: str,
    ) -> pl.DataFrame:
        """
        Read all Parquet files from a specific partition.

        Useful for testing and verification.

        Args:
            dataset_id: Dataset identifier
            domain_prefix: Domain prefix

        Returns:
            Combined DataFrame from all partition files

        Raises:
            FileNotFoundError: If partition doesn't exist
        """
        partition_path = self.layout.get_partition_path(
            dataset_id, domain_prefix
        )

        if not partition_path.exists():
            raise FileNotFoundError(
                f"Partition not found: {partition_path}"
            )

        # Read all parquet files in partition
        parquet_files = self.layout.list_parquet_files(
            dataset_id, domain_prefix
        )

        if not parquet_files:
            # Return empty DataFrame with correct schema
            return pl.DataFrame(
                schema={
                    "domain_id": pl.Int64,
                    "url_id": pl.Int64,
                    "scheme": pl.Utf8,
                    "host": pl.Utf8,
                    "path_query": pl.Utf8,
                    "domain": pl.Utf8,
                }
            )

        # Read and concatenate all files
        dfs = []
        for file_path in parquet_files:
            df = pl.read_parquet(file_path)
            dfs.append(df)

        return pl.concat(dfs)
