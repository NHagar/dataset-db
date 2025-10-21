"""Unit tests for ID generation."""

import pytest

from dataset_db.normalization import IDGenerator, get_id_generator, reset_id_generator


class TestIDGenerator:
    """Test suite for IDGenerator."""

    @pytest.fixture
    def id_gen(self):
        """Create a fresh IDGenerator instance."""
        reset_id_generator()
        return IDGenerator()

    def test_url_id_generation(self, id_gen):
        """Test URL ID generation is consistent."""
        url = "https://example.com/path"

        id1 = id_gen.get_url_id(url)
        id2 = id_gen.get_url_id(url)

        # Same URL should produce same ID
        assert id1 == id2
        assert isinstance(id1, int)

    def test_url_id_different_urls(self, id_gen):
        """Test different URLs produce different IDs."""
        url1 = "https://example.com/path1"
        url2 = "https://example.com/path2"

        id1 = id_gen.get_url_id(url1)
        id2 = id_gen.get_url_id(url2)

        # Different URLs should (very likely) produce different IDs
        assert id1 != id2

    def test_url_id_range(self, id_gen):
        """Test URL IDs are in signed int64 range."""
        url = "https://example.com/path"
        url_id = id_gen.get_url_id(url)

        # Should fit in signed int64
        assert -(2**63) <= url_id < 2**63

    def test_domain_id_generation(self, id_gen):
        """Test domain ID generation is consistent."""
        domain = "example.com"

        id1 = id_gen.get_domain_id(domain)
        id2 = id_gen.get_domain_id(domain)

        # Same domain should produce same ID
        assert id1 == id2
        assert isinstance(id1, int)

    def test_domain_id_different_domains(self, id_gen):
        """Test different domains produce different IDs."""
        domain1 = "example.com"
        domain2 = "example.org"

        id1 = id_gen.get_domain_id(domain1)
        id2 = id_gen.get_domain_id(domain2)

        # Different domains should (very likely) produce different IDs
        assert id1 != id2

    def test_domain_prefix(self, id_gen):
        """Test domain prefix generation."""
        domain = "example.com"

        # Default 2 chars
        prefix = id_gen.get_domain_prefix(domain)
        assert len(prefix) == 2
        assert all(c in "0123456789abcdef" for c in prefix)

        # Custom length
        prefix4 = id_gen.get_domain_prefix(domain, prefix_chars=4)
        assert len(prefix4) == 4

    def test_domain_prefix_consistent(self, id_gen):
        """Test domain prefix is consistent."""
        domain = "example.com"

        prefix1 = id_gen.get_domain_prefix(domain)
        prefix2 = id_gen.get_domain_prefix(domain)

        assert prefix1 == prefix2

    def test_dataset_registration(self, id_gen):
        """Test dataset registration."""
        dataset_id = id_gen.register_dataset("test_dataset")

        assert isinstance(dataset_id, int)
        assert dataset_id >= 0
        assert dataset_id < 2**32

    def test_dataset_registration_idempotent(self, id_gen):
        """Test registering same dataset returns same ID."""
        id1 = id_gen.register_dataset("test_dataset")
        id2 = id_gen.register_dataset("test_dataset")

        assert id1 == id2

    def test_dataset_registration_sequential(self, id_gen):
        """Test dataset IDs are assigned sequentially."""
        id1 = id_gen.register_dataset("dataset1")
        id2 = id_gen.register_dataset("dataset2")
        id3 = id_gen.register_dataset("dataset3")

        assert id2 == id1 + 1
        assert id3 == id2 + 1

    def test_get_dataset_id(self, id_gen):
        """Test getting dataset ID."""
        id_gen.register_dataset("test_dataset")
        dataset_id = id_gen.get_dataset_id("test_dataset")

        assert isinstance(dataset_id, int)

    def test_get_dataset_id_not_registered(self, id_gen):
        """Test getting unregistered dataset raises KeyError."""
        with pytest.raises(KeyError):
            id_gen.get_dataset_id("unknown_dataset")

    def test_get_all_datasets(self, id_gen):
        """Test getting all registered datasets."""
        id_gen.register_dataset("dataset1")
        id_gen.register_dataset("dataset2")

        datasets = id_gen.get_all_datasets()

        assert len(datasets) == 2
        assert "dataset1" in datasets
        assert "dataset2" in datasets
        assert isinstance(datasets["dataset1"], int)


class TestGlobalIDGenerator:
    """Test global ID generator singleton."""

    def test_get_id_generator(self):
        """Test getting global ID generator."""
        reset_id_generator()

        gen1 = get_id_generator()
        gen2 = get_id_generator()

        # Should return same instance
        assert gen1 is gen2

    def test_reset_id_generator(self):
        """Test resetting global ID generator."""
        gen1 = get_id_generator()
        gen1.register_dataset("test")

        reset_id_generator()
        gen2 = get_id_generator()

        # Should be a new instance
        assert gen1 is not gen2

        # New instance should not have old datasets
        with pytest.raises(KeyError):
            gen2.get_dataset_id("test")
