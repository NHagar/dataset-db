"""
Unit tests for ParquetWriter class.
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from dataset_db.storage import ParquetWriter


@pytest.fixture
def temp_storage():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def writer(temp_storage):
    """Create a ParquetWriter instance with temp storage."""
    return ParquetWriter(
        base_path=temp_storage,
        compression="zstd",
        compression_level=6,
    )


@pytest.fixture
def sample_normalized_df():
    """Create a sample normalized DataFrame."""
    return pl.DataFrame({
        "dataset_id": [1, 1, 1, 1],
        "domain_id": [100, 100, 200, 200],
        "url_id": [1001, 1002, 2001, 2002],
        "scheme": ["https", "https", "http", "https"],
        "host": ["example.com", "example.com", "another.org", "another.org"],
        "path_query": ["/path1", "/path2", "/test", "/page"],
        "domain": ["example.com", "example.com", "another.org", "another.org"],
        "domain_prefix": ["3a", "3a", "5b", "5b"],
    }).with_columns([
        pl.col("dataset_id").cast(pl.Int32),
        pl.col("domain_id").cast(pl.Int64),
        pl.col("url_id").cast(pl.Int64),
    ])


class TestParquetWriterBasic:
    """Test basic ParquetWriter functionality."""

    def test_initialization(self, temp_storage):
        """Test ParquetWriter initialization."""
        writer = ParquetWriter(
            base_path=temp_storage,
            compression="zstd",
            compression_level=6,
        )

        assert writer.base_path == temp_storage
        assert writer.compression == "zstd"
        assert writer.compression_level == 6
        assert writer.layout.base_path == temp_storage

    def test_initialization_with_defaults(self):
        """Test initialization with default config."""
        writer = ParquetWriter()
        assert writer.compression is not None
        assert writer.row_group_size > 0

    def test_initial_stats(self, writer):
        """Test initial statistics are zero."""
        stats = writer.get_stats()

        assert stats["batches_written"] == 0
        assert stats["rows_written"] == 0
        assert stats["bytes_written"] == 0
        assert stats["files_created"] == 0


class TestParquetWriterValidation:
    """Test schema validation."""

    def test_validate_correct_schema(self, writer, sample_normalized_df):
        """Test validation passes for correct schema."""
        # Should not raise
        writer._validate_schema(sample_normalized_df)

    def test_validate_missing_columns(self, writer):
        """Test validation fails for missing columns."""
        df = pl.DataFrame({
            "dataset_id": [1],
            "domain_id": [100],
            # Missing other required columns
        })

        with pytest.raises(ValueError, match="missing required columns"):
            writer._validate_schema(df)

    def test_validate_wrong_types(self, writer):
        """Test validation fails for incorrect types."""
        df = pl.DataFrame({
            "dataset_id": [1],
            "domain_id": [100],
            "url_id": [1001],
            "scheme": ["https"],
            "host": ["example.com"],
            "path_query": ["/path"],
            "domain": ["example.com"],
            "domain_prefix": ["3a"],
        }).with_columns([
            pl.col("dataset_id").cast(pl.Int64),  # Wrong! Should be Int32
            pl.col("domain_id").cast(pl.Int64),
            pl.col("url_id").cast(pl.Int64),
        ])

        with pytest.raises(ValueError, match="has type"):
            writer._validate_schema(df)


class TestParquetWriterWrite:
    """Test writing functionality."""

    def test_write_empty_dataframe(self, writer):
        """Test writing empty DataFrame."""
        df = pl.DataFrame(schema={
            "dataset_id": pl.Int32,
            "domain_id": pl.Int64,
            "url_id": pl.Int64,
            "scheme": pl.Utf8,
            "host": pl.Utf8,
            "path_query": pl.Utf8,
            "domain": pl.Utf8,
            "domain_prefix": pl.Utf8,
        })

        result = writer.write_batch(df)

        assert result["files_written"] == 0
        assert result["rows_written"] == 0

    def test_write_single_partition(self, writer, sample_normalized_df):
        """Test writing data to single partition."""
        # Filter to single partition
        df = sample_normalized_df.filter(
            pl.col("domain_prefix") == "3a"
        )

        result = writer.write_batch(df)

        assert result["files_written"] == 1
        assert result["rows_written"] == 2

        # Verify file exists
        parquet_path = writer.layout.get_parquet_path(1, "3a", 0)
        assert parquet_path.exists()

    def test_write_multiple_partitions(self, writer, sample_normalized_df):
        """Test writing data to multiple partitions."""
        result = writer.write_batch(sample_normalized_df)

        # Should create 2 files (2 different domain_prefixes)
        assert result["files_written"] == 2
        assert result["rows_written"] == 4

        # Verify both partition files exist
        path_3a = writer.layout.get_parquet_path(1, "3a", 0)
        path_5b = writer.layout.get_parquet_path(1, "5b", 0)

        assert path_3a.exists()
        assert path_5b.exists()

    def test_write_incremental(self, writer, sample_normalized_df):
        """Test incremental writes create new part files."""
        # First write
        result1 = writer.write_batch(sample_normalized_df)
        assert result1["files_written"] == 2

        # Second write should create new parts
        result2 = writer.write_batch(sample_normalized_df)
        assert result2["files_written"] == 2

        # Check that multiple parts exist
        files_3a = writer.layout.list_parquet_files(1, "3a")
        files_5b = writer.layout.list_parquet_files(1, "5b")

        assert len(files_3a) == 2  # part-00000 and part-00001
        assert len(files_5b) == 2

    def test_write_updates_stats(self, writer, sample_normalized_df):
        """Test that writes update statistics."""
        initial_stats = writer.get_stats()
        assert initial_stats["batches_written"] == 0

        writer.write_batch(sample_normalized_df)

        stats = writer.get_stats()
        assert stats["batches_written"] == 1
        assert stats["rows_written"] == 4
        assert stats["files_created"] == 2
        assert stats["bytes_written"] > 0

    def test_write_with_dataset_filter(self, writer):
        """Test filtering by dataset_id during write."""
        df = pl.DataFrame({
            "dataset_id": [1, 1, 2, 2],
            "domain_id": [100, 100, 200, 200],
            "url_id": [1001, 1002, 2001, 2002],
            "scheme": ["https"] * 4,
            "host": ["example.com"] * 4,
            "path_query": ["/path1", "/path2", "/path3", "/path4"],
            "domain": ["example.com"] * 4,
            "domain_prefix": ["3a"] * 4,
        }).with_columns([
            pl.col("dataset_id").cast(pl.Int32),
            pl.col("domain_id").cast(pl.Int64),
            pl.col("url_id").cast(pl.Int64),
        ])

        result = writer.write_batch(df, dataset_id=1)

        # Should only write 2 rows (dataset_id=1)
        assert result["rows_written"] == 2


class TestParquetWriterRead:
    """Test reading functionality."""

    def test_read_partition(self, writer, sample_normalized_df):
        """Test reading data back from partition."""
        # Write data
        writer.write_batch(sample_normalized_df)

        # Read back
        df = writer.read_partition(1, "3a")

        assert len(df) == 2
        assert set(df.columns) == {
            "domain_id",
            "url_id",
            "scheme",
            "host",
            "path_query",
            "domain",
        }

        # Note: partition columns (dataset_id, domain_prefix) not in output
        assert "dataset_id" not in df.columns
        assert "domain_prefix" not in df.columns

    def test_read_nonexistent_partition(self, writer):
        """Test reading from nonexistent partition raises error."""
        with pytest.raises(FileNotFoundError):
            writer.read_partition(99, "zz")

    def test_read_empty_partition(self, writer, temp_storage):
        """Test reading from empty partition returns empty DataFrame."""
        # Create partition directory but no files
        writer.layout.ensure_partition_exists(1, "3a")

        df = writer.read_partition(1, "3a")

        assert len(df) == 0
        assert set(df.columns) == {
            "domain_id",
            "url_id",
            "scheme",
            "host",
            "path_query",
            "domain",
        }

    def test_read_multiple_files(self, writer, sample_normalized_df):
        """Test reading partition with multiple files."""
        # Write same data twice to create multiple files
        writer.write_batch(sample_normalized_df)
        writer.write_batch(sample_normalized_df)

        # Read back - should concatenate all files
        df = writer.read_partition(1, "3a")

        # Should have data from both writes
        assert len(df) == 4  # 2 rows * 2 writes

    def test_roundtrip_data_integrity(self, writer, sample_normalized_df):
        """Test that data survives write-read roundtrip."""
        # Write data
        writer.write_batch(sample_normalized_df)

        # Read back
        df_3a = writer.read_partition(1, "3a")
        df_5b = writer.read_partition(1, "5b")

        # Combine and compare (excluding partition columns)
        df_combined = pl.concat([df_3a, df_5b])

        # Sort for comparison
        original_sorted = sample_normalized_df.select([
            "domain_id",
            "url_id",
            "scheme",
            "host",
            "path_query",
            "domain",
        ]).sort("url_id")

        read_sorted = df_combined.sort("url_id")

        assert original_sorted.equals(read_sorted)


class TestParquetWriterCompression:
    """Test compression and encoding."""

    def test_compression_applied(self, writer, sample_normalized_df):
        """Test that compression is applied to files."""
        writer.write_batch(sample_normalized_df)

        # Check file size is reasonable (compressed)
        path = writer.layout.get_parquet_path(1, "3a", 0)
        file_size = path.stat().st_size

        # File should be relatively small due to compression
        # (Hard to test exact size, but shouldn't be huge)
        assert file_size < 10_000  # Sanity check

    def test_dictionary_encoding_applied(self, writer, temp_storage):
        """Test that dictionary encoding is used."""
        # Create data with repeated strings (good for dictionary encoding)
        df = pl.DataFrame({
            "dataset_id": [1] * 100,
            "domain_id": [100] * 100,
            "url_id": list(range(1000, 1100)),
            "scheme": ["https"] * 100,  # High repetition
            "host": ["example.com"] * 100,  # High repetition
            "path_query": [f"/path{i}" for i in range(100)],
            "domain": ["example.com"] * 100,  # High repetition
            "domain_prefix": ["3a"] * 100,
        }).with_columns([
            pl.col("dataset_id").cast(pl.Int32),
            pl.col("domain_id").cast(pl.Int64),
            pl.col("url_id").cast(pl.Int64),
        ])

        writer.write_batch(df)

        path = writer.layout.get_parquet_path(1, "3a", 0)
        file_size = path.stat().st_size

        # With dictionary encoding, file should be quite small
        # 100 rows but lots of repetition
        assert file_size < 5_000  # Should be heavily compressed


class TestParquetWriterRowGroups:
    """Test row group size management."""

    def test_estimate_row_group_rows_empty(self, writer):
        """Test row group estimation with empty DataFrame."""
        df = pl.DataFrame(schema={
            "dataset_id": pl.Int32,
            "domain_id": pl.Int64,
            "url_id": pl.Int64,
            "scheme": pl.Utf8,
            "host": pl.Utf8,
            "path_query": pl.Utf8,
            "domain": pl.Utf8,
            "domain_prefix": pl.Utf8,
        })

        rows = writer._estimate_row_group_rows(df)
        assert rows == 1000  # Default minimum

    def test_estimate_row_group_rows_small(self, writer, sample_normalized_df):
        """Test row group estimation with small DataFrame."""
        rows = writer._estimate_row_group_rows(sample_normalized_df)

        # Should return reasonable value
        assert rows >= 1000
        assert rows <= 1_000_000

    def test_estimate_row_group_rows_large(self, writer):
        """Test row group estimation with larger DataFrame."""
        df = pl.DataFrame({
            "dataset_id": [1] * 10_000,
            "domain_id": list(range(10_000)),
            "url_id": list(range(10_000)),
            "scheme": ["https"] * 10_000,
            "host": [f"host{i}.com" for i in range(10_000)],
            "path_query": [f"/path{i}" for i in range(10_000)],
            "domain": [f"host{i}.com" for i in range(10_000)],
            "domain_prefix": ["3a"] * 10_000,
        }).with_columns([
            pl.col("dataset_id").cast(pl.Int32),
            pl.col("domain_id").cast(pl.Int64),
            pl.col("url_id").cast(pl.Int64),
        ])

        rows = writer._estimate_row_group_rows(df)

        # Should return reasonable value
        assert rows >= 1000
        assert rows <= 1_000_000


class TestParquetWriterStorageStats:
    """Test storage statistics."""

    def test_get_storage_stats_empty(self, writer):
        """Test storage stats when empty."""
        stats = writer.get_storage_stats()

        assert stats["total_partitions"] == 0
        assert stats["total_files"] == 0
        assert stats["total_size_bytes"] == 0

    def test_get_storage_stats_after_write(
        self, writer, sample_normalized_df
    ):
        """Test storage stats after writing."""
        writer.write_batch(sample_normalized_df)

        stats = writer.get_storage_stats()

        assert stats["total_partitions"] == 2
        assert stats["total_files"] == 2
        assert stats["total_size_bytes"] > 0


class TestParquetWriterIntegration:
    """Integration tests with full pipeline."""

    def test_multiple_datasets(self, writer):
        """Test writing multiple datasets."""
        # Dataset 1
        df1 = pl.DataFrame({
            "dataset_id": [1] * 3,
            "domain_id": [100, 100, 200],
            "url_id": [1001, 1002, 2001],
            "scheme": ["https"] * 3,
            "host": ["example.com"] * 3,
            "path_query": ["/a", "/b", "/c"],
            "domain": ["example.com"] * 3,
            "domain_prefix": ["3a", "3a", "5b"],
        }).with_columns([
            pl.col("dataset_id").cast(pl.Int32),
            pl.col("domain_id").cast(pl.Int64),
            pl.col("url_id").cast(pl.Int64),
        ])

        # Dataset 2
        df2 = pl.DataFrame({
            "dataset_id": [2] * 3,
            "domain_id": [300, 300, 400],
            "url_id": [3001, 3002, 4001],
            "scheme": ["https"] * 3,
            "host": ["another.org"] * 3,
            "path_query": ["/x", "/y", "/z"],
            "domain": ["another.org"] * 3,
            "domain_prefix": ["7c", "7c", "9d"],
        }).with_columns([
            pl.col("dataset_id").cast(pl.Int32),
            pl.col("domain_id").cast(pl.Int64),
            pl.col("url_id").cast(pl.Int64),
        ])

        writer.write_batch(df1)
        writer.write_batch(df2)

        stats = writer.get_storage_stats()

        assert len(stats["datasets"]) == 2
        assert stats["total_partitions"] == 4  # 2 + 2
        assert stats["total_files"] == 4

    def test_large_batch_partitioning(self, writer):
        """Test partitioning with larger batch."""
        # Generate diverse domain prefixes
        prefixes = [f"{i:02x}" for i in range(16)]

        df = pl.DataFrame({
            "dataset_id": [1] * 160,
            "domain_id": list(range(160)),
            "url_id": list(range(160)),
            "scheme": ["https"] * 160,
            "host": [f"host{i}.com" for i in range(160)],
            "path_query": [f"/path{i}" for i in range(160)],
            "domain": [f"host{i}.com" for i in range(160)],
            "domain_prefix": prefixes * 10,  # 16 prefixes * 10 = 160
        }).with_columns([
            pl.col("dataset_id").cast(pl.Int32),
            pl.col("domain_id").cast(pl.Int64),
            pl.col("url_id").cast(pl.Int64),
        ])

        result = writer.write_batch(df)

        # Should create 16 files (one per domain prefix)
        assert result["files_written"] == 16
        assert result["rows_written"] == 160
