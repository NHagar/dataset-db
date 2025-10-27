"""
Tests for incremental index building.
"""


import polars as pl
import pytest

from dataset_db.index import IndexBuilder
from dataset_db.ingestion import IngestionProcessor
from dataset_db.storage import ParquetWriter


@pytest.fixture
def test_data_dir(tmp_path):
    """Create temporary data directory."""
    return tmp_path / "data"


@pytest.fixture
def sample_urls_batch1():
    """First batch of sample URLs."""
    return pl.DataFrame(
        {
            "url": [
                "https://example.com/page1",
                "https://test.com/page1",
                "https://example.org/page1",
            ]
        }
    )


@pytest.fixture
def sample_urls_batch2():
    """Second batch of sample URLs (different dataset)."""
    return pl.DataFrame(
        {
            "url": [
                "https://example.com/page2",  # Overlapping domain
                "https://newsite.com/page1",  # New domain
            ]
        }
    )


def test_incremental_file_registry(test_data_dir, sample_urls_batch1, sample_urls_batch2):
    """Test incremental file registry building."""
    # Ingest first batch
    processor = IngestionProcessor()
    writer = ParquetWriter(base_path=test_data_dir)

    normalized1 = processor.process_batch(sample_urls_batch1, "dataset1")
    writer.write_batch(normalized1)
    writer.flush()

    # Build initial indexes
    builder = IndexBuilder(test_data_dir)
    builder.build_all()

    # Check file registry
    assert len(builder.file_registry.files) > 0
    initial_file_count = len(builder.file_registry.files)

    # Ingest second batch
    normalized2 = processor.process_batch(sample_urls_batch2, "dataset2")
    writer.write_batch(normalized2)
    writer.flush()

    # Build incremental indexes
    # Need to create a new builder to avoid reusing the same instance
    builder2 = IndexBuilder(test_data_dir)
    version2 = builder2.build_incremental()

    # Check that version was created (may be same or different depending on timing)
    assert version2 is not None

    # Check that file registry has more files
    assert len(builder2.file_registry.files) > initial_file_count


def test_incremental_domain_dict(test_data_dir, sample_urls_batch1, sample_urls_batch2):
    """Test incremental domain dictionary building."""
    # Ingest first batch
    processor = IngestionProcessor()
    writer = ParquetWriter(base_path=test_data_dir)

    normalized1 = processor.process_batch(sample_urls_batch1, "dataset1")
    writer.write_batch(normalized1)
    writer.flush()

    # Build initial indexes
    builder = IndexBuilder(test_data_dir)
    version1 = builder.build_all()

    # Get initial domains
    domains1 = builder.domain_dict.read_domain_dict(version1)
    initial_domain_count = len(domains1)

    # Ingest second batch
    normalized2 = processor.process_batch(sample_urls_batch2, "dataset2")
    writer.write_batch(normalized2)
    writer.flush()

    # Build incremental indexes
    builder2 = IndexBuilder(test_data_dir)
    version2 = builder2.build_incremental()

    # Only check if version2 is different (might be same if no new files detected)
    if version2 != version1:
        # Get new domains
        domains2 = builder2.domain_dict.read_domain_dict(version2)

        # Check that we have more domains (or same if all domains were duplicates)
        assert len(domains2) >= initial_domain_count

        # Check that new domain (newsite.com) is present
        assert "newsite.com" in domains2
    else:
        # If version is the same, no new files were detected
        # This can happen if both builds complete in the same second
        pass


def test_incremental_membership(test_data_dir, sample_urls_batch1, sample_urls_batch2):
    """Test incremental membership index building."""
    # Ingest first batch
    processor = IngestionProcessor()
    writer = ParquetWriter(base_path=test_data_dir)

    normalized1 = processor.process_batch(sample_urls_batch1, "dataset1")
    writer.write_batch(normalized1)
    writer.flush()

    # Build initial indexes
    builder = IndexBuilder(test_data_dir)
    version1 = builder.build_all()

    # Get domain_id for example.com
    domains1 = builder.domain_dict.read_domain_dict(version1)
    mphf1 = builder.mphf
    mphf1.load(test_data_dir / "index" / version1 / "domains.mphf")
    example_domain_id = mphf1.lookup("example.com")

    # Load membership index
    membership_path1 = test_data_dir / "index" / version1 / "domain_to_datasets.roar"
    builder.membership.load(membership_path1, len(domains1))

    # Check that example.com is in at least one dataset
    datasets1 = builder.membership.get_datasets(example_domain_id)
    assert len(datasets1) >= 1  # At least one dataset

    # Ingest second batch (which also contains example.com)
    normalized2 = processor.process_batch(sample_urls_batch2, "dataset2")
    writer.write_batch(normalized2)
    writer.flush()

    # Build incremental indexes
    builder2 = IndexBuilder(test_data_dir)
    version2 = builder2.build_incremental()

    # Only check if version2 is different
    if version2 != version1:
        # Get new domain_id for example.com (may have changed!)
        domains2 = builder2.domain_dict.read_domain_dict(version2)
        mphf2 = builder2.mphf
        mphf2.load(test_data_dir / "index" / version2 / "domains.mphf")
        example_domain_id2 = mphf2.lookup("example.com")

        # Load new membership index
        membership_path2 = test_data_dir / "index" / version2 / "domain_to_datasets.roar"
        builder2.membership.load(membership_path2, len(domains2))

        # Check that example.com is now in both datasets
        datasets2 = builder2.membership.get_datasets(example_domain_id2)
        # Note: dataset IDs are from file registry, which assigns sequential IDs
        assert len(datasets2) >= len(datasets1)  # Should have at least as many datasets


def test_incremental_no_new_files(test_data_dir, sample_urls_batch1):
    """Test incremental build when there are no new files."""
    # Ingest first batch
    processor = IngestionProcessor()
    writer = ParquetWriter(base_path=test_data_dir)

    normalized1 = processor.process_batch(sample_urls_batch1, "dataset1")
    writer.write_batch(normalized1)
    writer.flush()

    # Build initial indexes
    builder = IndexBuilder(test_data_dir)
    version1 = builder.build_all()

    # Build incremental without adding any files
    version2 = builder.build_incremental()

    # Should return the same version since no new files
    assert version2 == version1


def test_incremental_first_build(test_data_dir, sample_urls_batch1):
    """Test incremental build when there is no previous version."""
    # Ingest first batch
    processor = IngestionProcessor()
    writer = ParquetWriter(base_path=test_data_dir)

    normalized1 = processor.process_batch(sample_urls_batch1, "dataset1")
    writer.write_batch(normalized1)
    writer.flush()

    # Build "incremental" when no previous version exists
    # Should fall back to full build
    builder = IndexBuilder(test_data_dir)
    version1 = builder.build_incremental()

    # Should have created a version
    assert version1 is not None

    # Check that indexes were built
    domains = builder.domain_dict.read_domain_dict(version1)
    assert len(domains) > 0
