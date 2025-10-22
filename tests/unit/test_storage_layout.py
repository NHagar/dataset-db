"""
Unit tests for StorageLayout class.
"""

import tempfile
from pathlib import Path

import pytest

from dataset_db.storage.layout import StorageLayout


@pytest.fixture
def temp_storage():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def layout(temp_storage):
    """Create a StorageLayout instance with temp storage."""
    return StorageLayout(temp_storage)


class TestStorageLayoutBasic:
    """Test basic StorageLayout functionality."""

    def test_initialization(self, temp_storage):
        """Test StorageLayout initialization."""
        layout = StorageLayout(temp_storage)
        assert layout.base_path == temp_storage
        assert layout.urls_root == temp_storage / "urls"

    def test_get_partition_path(self, layout):
        """Test partition path generation."""
        path = layout.get_partition_path(17, "3a")
        expected = layout.urls_root / "dataset_id=17" / "domain_prefix=3a"
        assert path == expected

    def test_get_parquet_path(self, layout):
        """Test Parquet file path generation."""
        path = layout.get_parquet_path(17, "3a", 0)
        expected = (
            layout.urls_root
            / "dataset_id=17"
            / "domain_prefix=3a"
            / "part-00000.parquet"
        )
        assert path == expected

    def test_part_number_formatting(self, layout):
        """Test part number formatting with leading zeros."""
        path0 = layout.get_parquet_path(1, "aa", 0)
        assert path0.name == "part-00000.parquet"

        path123 = layout.get_parquet_path(1, "aa", 123)
        assert path123.name == "part-00123.parquet"

        path99999 = layout.get_parquet_path(1, "aa", 99999)
        assert path99999.name == "part-99999.parquet"


class TestStorageLayoutDirectories:
    """Test directory creation and management."""

    def test_ensure_partition_exists_creates_directory(self, layout):
        """Test that ensure_partition_exists creates directories."""
        path = layout.ensure_partition_exists(17, "3a")

        assert path.exists()
        assert path.is_dir()
        assert path == layout.get_partition_path(17, "3a")

    def test_ensure_partition_exists_idempotent(self, layout):
        """Test that ensure_partition_exists is idempotent."""
        path1 = layout.ensure_partition_exists(17, "3a")
        path2 = layout.ensure_partition_exists(17, "3a")

        assert path1 == path2
        assert path1.exists()

    def test_ensure_partition_creates_parent_directories(self, layout):
        """Test that all parent directories are created."""
        path = layout.ensure_partition_exists(17, "3a")

        assert layout.urls_root.exists()
        assert (layout.urls_root / "dataset_id=17").exists()
        assert path.exists()


class TestStorageLayoutListing:
    """Test partition and file listing functionality."""

    def test_list_partitions_empty(self, layout):
        """Test listing partitions when none exist."""
        partitions = layout.list_partitions()
        assert partitions == []

    def test_list_partitions_single(self, layout):
        """Test listing single partition."""
        layout.ensure_partition_exists(17, "3a")

        partitions = layout.list_partitions()
        assert len(partitions) == 1
        assert partitions[0] == (17, "3a")

    def test_list_partitions_multiple(self, layout):
        """Test listing multiple partitions."""
        layout.ensure_partition_exists(17, "3a")
        layout.ensure_partition_exists(17, "5b")
        layout.ensure_partition_exists(23, "3a")

        partitions = layout.list_partitions()
        assert len(partitions) == 3
        assert (17, "3a") in partitions
        assert (17, "5b") in partitions
        assert (23, "3a") in partitions

    def test_list_partitions_sorted(self, layout):
        """Test that partitions are sorted."""
        layout.ensure_partition_exists(23, "ff")
        layout.ensure_partition_exists(17, "3a")
        layout.ensure_partition_exists(17, "5b")

        partitions = layout.list_partitions()
        assert partitions == sorted(partitions)

    def test_list_partitions_filter_by_dataset(self, layout):
        """Test filtering partitions by dataset_id."""
        layout.ensure_partition_exists(17, "3a")
        layout.ensure_partition_exists(17, "5b")
        layout.ensure_partition_exists(23, "3a")

        partitions = layout.list_partitions(dataset_id=17)
        assert len(partitions) == 2
        assert all(ds_id == 17 for ds_id, _ in partitions)

    def test_list_parquet_files_empty(self, layout):
        """Test listing files when partition is empty."""
        layout.ensure_partition_exists(17, "3a")

        files = layout.list_parquet_files(17, "3a")
        assert files == []

    def test_list_parquet_files_nonexistent_partition(self, layout):
        """Test listing files for nonexistent partition."""
        files = layout.list_parquet_files(99, "zz")
        assert files == []

    def test_list_parquet_files_with_files(self, layout):
        """Test listing files when files exist."""
        layout.ensure_partition_exists(17, "3a")

        # Create some dummy files
        partition_path = layout.get_partition_path(17, "3a")
        (partition_path / "part-00000.parquet").touch()
        (partition_path / "part-00001.parquet").touch()

        files = layout.list_parquet_files(17, "3a")
        assert len(files) == 2
        assert all(f.suffix == ".parquet" for f in files)

    def test_list_parquet_files_ignores_non_parquet(self, layout):
        """Test that non-parquet files are ignored."""
        layout.ensure_partition_exists(17, "3a")

        partition_path = layout.get_partition_path(17, "3a")
        (partition_path / "part-00000.parquet").touch()
        (partition_path / "other.txt").touch()
        (partition_path / ".hidden").touch()

        files = layout.list_parquet_files(17, "3a")
        assert len(files) == 1


class TestStorageLayoutPartNumbers:
    """Test part number generation."""

    def test_get_next_part_number_empty(self, layout):
        """Test next part number for empty partition."""
        layout.ensure_partition_exists(17, "3a")

        next_num = layout.get_next_part_number(17, "3a")
        assert next_num == 0

    def test_get_next_part_number_with_files(self, layout):
        """Test next part number when files exist."""
        layout.ensure_partition_exists(17, "3a")
        partition_path = layout.get_partition_path(17, "3a")

        (partition_path / "part-00000.parquet").touch()
        next_num = layout.get_next_part_number(17, "3a")
        assert next_num == 1

        (partition_path / "part-00001.parquet").touch()
        next_num = layout.get_next_part_number(17, "3a")
        assert next_num == 2

    def test_get_next_part_number_with_gaps(self, layout):
        """Test next part number with gaps in sequence."""
        layout.ensure_partition_exists(17, "3a")
        partition_path = layout.get_partition_path(17, "3a")

        (partition_path / "part-00000.parquet").touch()
        (partition_path / "part-00005.parquet").touch()

        # Should return max + 1, not fill gaps
        next_num = layout.get_next_part_number(17, "3a")
        assert next_num == 6

    def test_get_next_part_number_nonexistent_partition(self, layout):
        """Test next part number for nonexistent partition."""
        next_num = layout.get_next_part_number(99, "zz")
        assert next_num == 0


class TestStorageLayoutStats:
    """Test storage statistics."""

    def test_get_stats_empty(self, layout):
        """Test stats when storage is empty."""
        stats = layout.get_stats()

        assert stats["total_partitions"] == 0
        assert stats["total_files"] == 0
        assert stats["total_size_bytes"] == 0
        assert stats["datasets"] == []

    def test_get_stats_with_partitions(self, layout):
        """Test stats with partitions and files."""
        # Create partitions
        layout.ensure_partition_exists(17, "3a")
        layout.ensure_partition_exists(17, "5b")
        layout.ensure_partition_exists(23, "3a")

        # Create dummy files
        path1 = layout.get_partition_path(17, "3a")
        (path1 / "part-00000.parquet").write_text("a" * 100)

        path2 = layout.get_partition_path(17, "5b")
        (path2 / "part-00000.parquet").write_text("b" * 200)

        stats = layout.get_stats()

        assert stats["total_partitions"] == 3
        assert stats["total_files"] == 2
        assert stats["total_size_bytes"] == 300

        # Check per-dataset stats
        assert len(stats["datasets"]) == 2

        ds17 = next(d for d in stats["datasets"] if d["dataset_id"] == 17)
        assert ds17["partitions"] == 2
        assert ds17["files"] == 2

        ds23 = next(d for d in stats["datasets"] if d["dataset_id"] == 23)
        assert ds23["partitions"] == 1
        assert ds23["files"] == 0


class TestStorageLayoutEdgeCases:
    """Test edge cases and error handling."""

    def test_malformed_dataset_directory_ignored(self, layout, temp_storage):
        """Test that malformed dataset directories are ignored."""
        urls_root = temp_storage / "urls"
        urls_root.mkdir(parents=True)

        # Create valid and invalid directories
        (urls_root / "dataset_id=17").mkdir()
        (urls_root / "dataset_id=17" / "domain_prefix=3a").mkdir()
        (urls_root / "invalid_dir").mkdir()
        (urls_root / "dataset_id=").mkdir()  # Missing ID
        (urls_root / "dataset_id=abc").mkdir()  # Non-numeric ID

        partitions = layout.list_partitions()
        assert len(partitions) == 1
        assert partitions[0] == (17, "3a")

    def test_malformed_prefix_directory_ignored(self, layout, temp_storage):
        """Test that malformed prefix directories are ignored."""
        urls_root = temp_storage / "urls"
        dataset_dir = urls_root / "dataset_id=17"
        dataset_dir.mkdir(parents=True)

        # Create valid and invalid prefix directories
        (dataset_dir / "domain_prefix=3a").mkdir()
        (dataset_dir / "domain_prefix=").mkdir()  # Missing prefix
        (dataset_dir / "invalid_prefix").mkdir()

        partitions = layout.list_partitions()
        assert len(partitions) == 1
        assert partitions[0] == (17, "3a")
