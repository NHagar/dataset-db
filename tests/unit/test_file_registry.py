"""Tests for file registry."""


import polars as pl
import pytest

from dataset_db.index import FileRegistry
from dataset_db.storage import ParquetWriter


@pytest.fixture
def temp_data_path(tmp_path):
    """Create temporary data directory."""
    return tmp_path / "data"


@pytest.fixture
def sample_parquet_files(temp_data_path):
    """Create sample Parquet files for testing."""
    writer = ParquetWriter(base_path=temp_data_path)

    # Create test data
    df = pl.DataFrame(
        {
            "domain_id": [1, 2, 3],
            "url_id": [100, 200, 300],
            "scheme": ["https", "https", "http"],
            "host": ["example.com", "test.com", "demo.org"],
            "path_query": ["/", "/page", "/path"],
            "domain": ["example.com", "test.com", "demo.org"],
        }
    )

    # Write to different partitions
    writer._write_partition(df, dataset_id=0, domain_prefix="aa")
    writer._write_partition(df, dataset_id=0, domain_prefix="bb")
    writer._write_partition(df, dataset_id=1, domain_prefix="cc")

    return temp_data_path


def test_scan_parquet_files(sample_parquet_files):
    """Test scanning Parquet files and assigning file IDs."""
    registry = FileRegistry(sample_parquet_files)

    registry.scan_parquet_files()

    # Should have 3 files
    assert len(registry.files) == 3

    # Each file should have required fields
    for file_info in registry.files:
        assert "file_id" in file_info
        assert "dataset_id" in file_info
        assert "domain_prefix" in file_info
        assert "parquet_rel_path" in file_info


def test_save_and_load(sample_parquet_files):
    """Test saving and loading file registry (bug fix: CSV serialization)."""
    registry = FileRegistry(sample_parquet_files)
    registry.scan_parquet_files()

    version = "2025-01-01T00:00:00Z"
    output_path = sample_parquet_files / "index" / version / "files.tsv.zst"

    # Save - this should not raise AttributeError
    registry.save(output_path)

    assert output_path.exists()

    # Load into new instance
    registry2 = FileRegistry(sample_parquet_files)
    registry2.load(output_path)

    # Verify same data
    assert len(registry2.files) == len(registry.files)
    assert registry2.path_to_id == registry.path_to_id


def test_get_file_path(sample_parquet_files):
    """Test getting file path by file ID."""
    registry = FileRegistry(sample_parquet_files)
    registry.scan_parquet_files()

    # Get first file
    path = registry.get_file_path(0)
    assert path is not None
    assert "part-00000.parquet" in path


def test_get_file_info(sample_parquet_files):
    """Test getting complete file info."""
    registry = FileRegistry(sample_parquet_files)
    registry.scan_parquet_files()

    info = registry.get_file_info(0)
    assert info is not None
    assert info["file_id"] == 0
    assert "dataset_id" in info
    assert "domain_prefix" in info


def test_get_file_id(sample_parquet_files):
    """Test getting file ID by path."""
    registry = FileRegistry(sample_parquet_files)
    registry.scan_parquet_files()

    # Get a path
    path = registry.get_file_path(0)
    assert path is not None

    # Look it up
    file_id = registry.get_file_id(path)
    assert file_id == 0


def test_build(sample_parquet_files):
    """Test full build pipeline."""
    registry = FileRegistry(sample_parquet_files)
    version = "2025-01-01T00:00:00Z"

    output_path = registry.build(version, sample_parquet_files)

    assert output_path.exists()
    assert len(registry.files) > 0


def test_empty_registry(temp_data_path):
    """Test registry with no files."""
    registry = FileRegistry(temp_data_path)

    # Create index directory
    (temp_data_path / "index" / "test").mkdir(parents=True, exist_ok=True)

    # Save should handle empty registry gracefully
    output_path = temp_data_path / "index" / "test" / "files.tsv.zst"
    registry.save(output_path)

    # File should not be created for empty registry
    assert not output_path.exists()


def test_get_file_path_invalid_id(sample_parquet_files):
    """Test getting file path with invalid ID."""
    registry = FileRegistry(sample_parquet_files)
    registry.scan_parquet_files()

    # Invalid IDs
    assert registry.get_file_path(-1) is None
    assert registry.get_file_path(999) is None


def test_get_file_id_nonexistent(sample_parquet_files):
    """Test getting file ID for nonexistent path."""
    registry = FileRegistry(sample_parquet_files)
    registry.scan_parquet_files()

    assert registry.get_file_id("nonexistent/path.parquet") is None


def test_load_nonexistent(temp_data_path):
    """Test error handling for nonexistent registry file."""
    registry = FileRegistry(temp_data_path)

    with pytest.raises(FileNotFoundError):
        registry.load(temp_data_path / "nonexistent.tsv.zst")


def test_compression_ratio(sample_parquet_files):
    """Test that compression provides good ratio."""
    registry = FileRegistry(sample_parquet_files)
    registry.scan_parquet_files()

    version = "2025-01-01T00:00:00Z"
    output_path = sample_parquet_files / "index" / version / "files.tsv.zst"

    registry.save(output_path)

    # File should be quite small due to compression
    file_size = output_path.stat().st_size

    # Should be less than 5KB for 3 files
    assert file_size < 5000
