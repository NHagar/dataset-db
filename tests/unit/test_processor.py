"""Unit tests for ingestion processor."""

import polars as pl
import pytest

from dataset_db.ingestion import DuplicateTracker, IngestionProcessor
from dataset_db.normalization import IDGenerator, URLNormalizer


class TestIngestionProcessor:
    """Test suite for IngestionProcessor."""

    @pytest.fixture
    def processor(self, tmp_path):
        """Create a fresh IngestionProcessor instance."""
        normalizer = URLNormalizer()
        id_gen = IDGenerator()
        tracker = DuplicateTracker(base_path=tmp_path / "tracker_state")
        return IngestionProcessor(
            normalizer=normalizer,
            id_generator=id_gen,
            duplicate_tracker=tracker,
        )

    def test_process_batch_basic(self, processor):
        """Test basic batch processing."""
        input_df = pl.DataFrame({
            "url": [
                "https://example.com/path1",
                "https://example.org/path2",
            ],
            "domain": ["example.com", "example.org"],  # Will be re-extracted
        })

        result = processor.process_batch(input_df, "test_dataset")

        # Check output schema
        assert "dataset_id" in result.columns
        assert "domain_id" in result.columns
        assert "url_id" in result.columns
        assert "scheme" in result.columns
        assert "host" in result.columns
        assert "path_query" in result.columns
        assert "domain" in result.columns
        assert "domain_prefix" in result.columns
        # raw_url removed for space efficiency

        # Check we got 2 records
        assert len(result) == 2

        # Check types
        assert result.schema["dataset_id"] == pl.Int32
        assert result.schema["domain_id"] == pl.Int64
        assert result.schema["url_id"] == pl.Int64

    def test_process_batch_normalization(self, processor):
        """Test URLs are properly normalized."""
        input_df = pl.DataFrame({
            "url": ["HTTPS://Example.COM:443/Path?z=1&a=2#frag"],
        })

        result = processor.process_batch(input_df, "test_dataset")

        # Check normalization results
        row = result.row(0, named=True)
        assert row["scheme"] == "https"
        assert row["host"] == "example.com"
        assert row["domain"] == "example.com"
        assert "/Path" in row["path_query"]
        assert "a=2" in row["path_query"]
        assert "z=1" in row["path_query"]

    def test_process_batch_dataset_id(self, processor):
        """Test dataset ID is assigned correctly."""
        input_df = pl.DataFrame({"url": ["https://example.com/1"]})

        result1 = processor.process_batch(input_df, "dataset1")
        result2 = processor.process_batch(input_df, "dataset2")
        result3 = processor.process_batch(
            pl.DataFrame({"url": ["https://example.com/2"]}), "dataset1"
        )

        # Different datasets should get different IDs
        assert result1["dataset_id"][0] != result2["dataset_id"][0]

        # Same dataset should get same ID
        assert result1["dataset_id"][0] == result3["dataset_id"][0]

    def test_process_batch_url_id_consistent(self, processor):
        """Test URL IDs remain deterministic while skipping duplicates."""
        url = "https://example.com/path"
        input_df = pl.DataFrame({"url": [url, url]})

        result = processor.process_batch(input_df, "test_dataset")

        # Duplicate URLs within the same batch are skipped entirely
        assert len(result) == 1
        assert result["url_id"][0] == processor.id_generator.get_url_id(url)

    def test_process_batch_domain_id_consistent(self, processor):
        """Test domain IDs are consistent for same domain."""
        input_df = pl.DataFrame({
            "url": [
                "https://example.com/path1",
                "https://example.com/path2",
                "https://www.example.com/path3",  # Same eTLD+1
            ]
        })

        result = processor.process_batch(input_df, "test_dataset")

        # All should have same domain_id (same eTLD+1)
        assert result["domain_id"][0] == result["domain_id"][1]
        assert result["domain_id"][0] == result["domain_id"][2]

    def test_process_batch_domain_prefix(self, processor):
        """Test domain prefix is generated."""
        input_df = pl.DataFrame({"url": ["https://example.com/path"]})

        result = processor.process_batch(input_df, "test_dataset")

        domain_prefix = result["domain_prefix"][0]
        assert len(domain_prefix) == 2  # Default 2 chars
        assert all(c in "0123456789abcdef" for c in domain_prefix)

    def test_process_batch_empty_urls(self, processor):
        """Test empty URLs are skipped."""
        input_df = pl.DataFrame({
            "url": ["https://example.com/valid", "", "https://example.org/valid2"]
        })

        result = processor.process_batch(input_df, "test_dataset")

        # Should only have 2 records (empty skipped)
        assert len(result) == 2
        assert "example.com" in result["domain"][0]
        assert "example.org" in result["domain"][1]

    def test_process_batch_invalid_urls(self, processor):
        """Test invalid URLs are skipped with warning."""
        input_df = pl.DataFrame({
            "url": [
                "https://example.com/valid",
                "not a url",
                "https://example.org/valid2",
            ]
        })

        # Should not raise, but skip invalid URL
        result = processor.process_batch(input_df, "test_dataset")

        # Should have at least the valid URLs
        assert len(result) >= 2

    def test_reconstruct_url(self, processor):
        """Test URL reconstruction from components."""
        url = "https://example.com/path?a=1"
        reconstructed = processor.reconstruct_url("https", "example.com", "/path?a=1")
        assert reconstructed == url

        # Test that we can reconstruct from processed data
        input_df = pl.DataFrame({"url": [url]})
        result = processor.process_batch(input_df, "test_dataset")

        row = result.row(0, named=True)
        reconstructed = processor.reconstruct_url(
            row["scheme"], row["host"], row["path_query"]
        )
        # Should match normalized URL (not necessarily original)
        assert reconstructed == "https://example.com/path?a=1"

    def test_get_stats(self, processor):
        """Test getting processor statistics."""
        input_df = pl.DataFrame({"url": ["https://example.com/1"]})

        processor.process_batch(input_df, "dataset1")
        processor.process_batch(input_df, "dataset2")

        stats = processor.get_stats()

        assert stats["datasets_processed"] == 2
        assert "dataset1" in stats["datasets"]
        assert "dataset2" in stats["datasets"]

    def test_empty_batch(self, processor):
        """Test processing empty batch."""
        input_df = pl.DataFrame({"url": []})

        result = processor.process_batch(input_df, "test_dataset")

        # Should return empty DataFrame with correct schema
        assert len(result) == 0
        assert "dataset_id" in result.columns
        assert result.schema["dataset_id"] == pl.Int32

    def test_duplicates_skipped_across_batches(self, tmp_path):
        """Previously ingested URLs should be skipped in later batches."""

        tracker_path = tmp_path / "tracker_state"
        normalizer = URLNormalizer()
        id_gen = IDGenerator()

        processor = IngestionProcessor(
            normalizer=normalizer,
            id_generator=id_gen,
            duplicate_tracker=DuplicateTracker(base_path=tracker_path),
        )

        url = "https://example.com/path"
        batch = pl.DataFrame({"url": [url]})

        first = processor.process_batch(batch, "dataset1")
        assert len(first) == 1

        second = processor.process_batch(batch, "dataset1")
        assert len(second) == 0

    def test_duplicates_persist_across_processors(self, tmp_path):
        """Tracking information survives processor re-instantiation."""

        tracker_path = tmp_path / "tracker_state"
        url = "https://example.com/path"

        processor1 = IngestionProcessor(
            duplicate_tracker=DuplicateTracker(base_path=tracker_path)
        )
        first = processor1.process_batch(pl.DataFrame({"url": [url]}), "dataset1")
        assert len(first) == 1

        processor2 = IngestionProcessor(
            duplicate_tracker=DuplicateTracker(base_path=tracker_path)
        )
        result = processor2.process_batch(pl.DataFrame({"url": [url]}), "dataset1")

        assert len(result) == 0
