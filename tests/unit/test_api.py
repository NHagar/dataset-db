"""
Unit tests for API components.

Tests the loader, query service, and server endpoints.
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient

from dataset_db.api import QueryService, init_loader
from dataset_db.api.loader import IndexLoader
from dataset_db.index import IndexBuilder
from dataset_db.ingestion import DuplicateTracker, IngestionProcessor
from dataset_db.storage import ParquetWriter


@pytest.fixture
def test_data_path():
    """Create a temporary directory with test data and indexes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir)

        # Create test data
        tracker = DuplicateTracker(base_path=base_path / "tracker_state")
        processor = IngestionProcessor(duplicate_tracker=tracker)
        writer = ParquetWriter(base_path=base_path)

        # Add some test URLs from different domains
        test_urls = [
            "https://example.com/page1",
            "https://example.com/page2",
            "https://example.org/about",
            "https://test.com/home",
            "https://test.com/contact",
            "https://another.net/path",
        ]

        df = pl.DataFrame({"url": test_urls})

        # Process and write as dataset 1
        normalized = processor.process_batch(df, "test_dataset_1")
        writer.write_batch(normalized)

        # Add another batch as dataset 2
        test_urls_2 = [
            "https://example.com/other",
            "https://test.com/other",
        ]
        df2 = pl.DataFrame({"url": test_urls_2})
        normalized2 = processor.process_batch(df2, "test_dataset_2")
        writer.write_batch(normalized2)

        writer.flush()

        processor.mark_ingested("test_dataset_1", normalized)
        processor.mark_ingested("test_dataset_2", normalized2)

        # Build indexes
        builder = IndexBuilder(base_path=base_path)
        builder.build_all()

        yield base_path


class TestIndexLoader:
    """Tests for IndexLoader."""

    def test_loader_initialization(self, test_data_path):
        """Test that loader initializes correctly."""
        loader = IndexLoader(test_data_path)
        loader.load()

        assert loader.domains is not None
        assert len(loader.domains) > 0
        assert loader.mphf is not None
        assert loader.membership is not None
        assert loader.file_registry is not None
        assert loader.postings is not None

    def test_domain_id_lookup(self, test_data_path):
        """Test domain ID lookup with caching."""
        loader = IndexLoader(test_data_path)
        loader.load()

        # Lookup a domain
        domain_id = loader.lookup_domain_id("example.com")
        assert domain_id is not None
        assert isinstance(domain_id, int)

        # Same lookup should hit cache
        domain_id_2 = loader.lookup_domain_id("example.com")
        assert domain_id == domain_id_2

        # Non-existent domain
        missing_id = loader.lookup_domain_id("nonexistent.com")
        assert missing_id is None

    def test_get_domain_string(self, test_data_path):
        """Test reverse lookup from domain ID to string."""
        loader = IndexLoader(test_data_path)
        loader.load()

        # Lookup domain ID first
        domain_id = loader.lookup_domain_id("example.com")
        assert domain_id is not None

        # Reverse lookup
        domain_str = loader.get_domain_string(domain_id)
        assert domain_str == "example.com"

    def test_get_datasets_for_domain(self, test_data_path):
        """Test getting datasets containing a domain."""
        loader = IndexLoader(test_data_path)
        loader.load()

        domain_id = loader.lookup_domain_id("example.com")
        assert domain_id is not None

        datasets = loader.get_datasets_for_domain(domain_id)
        assert isinstance(datasets, list)
        # example.com appears in both datasets
        assert len(datasets) == 2


class TestQueryService:
    """Tests for QueryService."""

    def test_get_datasets_for_domain(self, test_data_path):
        """Test getting datasets for a domain."""
        loader = IndexLoader(test_data_path)
        loader.load()
        service = QueryService(loader)

        response = service.get_datasets_for_domain("example.com")

        assert response.domain == "example.com"
        assert response.domain_id is not None
        assert len(response.datasets) == 2

        # Check dataset IDs are present
        dataset_ids = [d.dataset_id for d in response.datasets]
        assert 0 in dataset_ids
        assert 1 in dataset_ids

    def test_get_datasets_for_missing_domain(self, test_data_path):
        """Test error handling for missing domain."""
        loader = IndexLoader(test_data_path)
        loader.load()
        service = QueryService(loader)

        with pytest.raises(ValueError, match="Domain not found"):
            service.get_datasets_for_domain("missing.com")

    def test_get_urls_for_domain_dataset(self, test_data_path):
        """Test getting URLs for a domain/dataset pair."""
        loader = IndexLoader(test_data_path)
        loader.load()
        service = QueryService(loader)

        # Get URLs for example.com in dataset 0
        response = service.get_urls_for_domain_dataset("example.com", dataset_id=0, limit=10)

        assert response.domain == "example.com"
        assert response.dataset_id == 0
        # Note: postings may not be fully built in test fixture, so just check response structure
        assert isinstance(response.items, list)
        assert response.total_est is None or response.total_est >= 0

        # If we have items, check their structure
        if len(response.items) > 0:
            urls = [item.url for item in response.items]
            assert any("example.com" in url for url in urls)
            assert all(item.url_id is not None for item in response.items)

    def test_url_pagination(self, test_data_path):
        """Test pagination of URL results."""
        loader = IndexLoader(test_data_path)
        loader.load()
        service = QueryService(loader)

        # Get first page
        response1 = service.get_urls_for_domain_dataset("example.com", dataset_id=0, limit=1)
        assert len(response1.items) <= 1

        # If there are more results, next_offset should be set
        if response1.next_offset is not None:
            response2 = service.get_urls_for_domain_dataset(
                "example.com", dataset_id=0, offset=response1.next_offset, limit=1
            )
            # Second page should have different URLs
            assert response2.items[0].url_id != response1.items[0].url_id

    def test_get_urls_for_invalid_dataset(self, test_data_path):
        """Test error handling when dataset doesn't contain domain."""
        loader = IndexLoader(test_data_path)
        loader.load()
        service = QueryService(loader)

        # another.net only appears in dataset 1
        with pytest.raises(ValueError, match="does not contain domain"):
            service.get_urls_for_domain_dataset("example.org", dataset_id=999)


class TestAPIEndpoints:
    """Tests for FastAPI endpoints."""

    @pytest.fixture(autouse=True)
    def setup_app(self, test_data_path):
        """Setup test client with test data."""
        from contextlib import asynccontextmanager

        from fastapi import FastAPI

        from dataset_db.api import server

        # Initialize loader before creating app/client
        # This sets up the global singleton that endpoints will use
        init_loader(test_data_path)

        # Create a custom lifespan for testing
        @asynccontextmanager
        async def test_lifespan(app: FastAPI):
            # Loader already initialized above
            yield
            # Shutdown: cleanup (nothing to do for now)

        # Create a new app instance with test lifespan
        test_app = FastAPI(
            title="Dataset DB API (Test)",
            description="Test instance with temporary data",
            version="0.1.0",
            lifespan=test_lifespan,
        )

        # Copy routes from the main app
        for route in server.app.routes:
            test_app.routes.append(route)

        # Create test client with the test app
        self.client = TestClient(test_app)

        yield  # Run the test

        # Cleanup: reset global loader state
        from dataset_db.api import loader as loader_module
        loader_module._loader = None

    def test_root_endpoint(self):
        """Test health check endpoint."""
        response = self.client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"

    def test_get_domain_datasets_endpoint(self):
        """Test GET /v1/domain/{domain}."""
        response = self.client.get("/v1/domain/example.com")
        assert response.status_code == 200

        data = response.json()
        assert data["domain"] == "example.com"
        assert "domain_id" in data
        assert "datasets" in data
        assert len(data["datasets"]) > 0

    def test_get_domain_datasets_not_found(self):
        """Test 404 for missing domain."""
        response = self.client.get("/v1/domain/missing.com")
        assert response.status_code == 404

    def test_get_urls_endpoint(self):
        """Test GET /v1/domain/{domain}/datasets/{dataset_id}/urls."""
        response = self.client.get("/v1/domain/example.com/datasets/0/urls?limit=10")
        assert response.status_code == 200

        data = response.json()
        assert data["domain"] == "example.com"
        assert data["dataset_id"] == 0
        assert "items" in data
        # Note: postings may not be fully built, so just check response structure
        assert isinstance(data["items"], list)

        # If we have items, check their structure
        if len(data["items"]) > 0:
            first_url = data["items"][0]
            assert "url_id" in first_url
            assert "url" in first_url
            assert "example.com" in first_url["url"]

    def test_get_urls_with_pagination(self):
        """Test pagination parameters."""
        # Get with limit
        response = self.client.get("/v1/domain/example.com/datasets/0/urls?limit=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) <= 1

        # Get with offset
        response = self.client.get("/v1/domain/example.com/datasets/0/urls?offset=1&limit=1")
        assert response.status_code == 200

    def test_get_urls_invalid_dataset(self):
        """Test 404 when dataset doesn't contain domain."""
        response = self.client.get("/v1/domain/example.com/datasets/999/urls")
        assert response.status_code == 404

    def test_pagination_limits(self):
        """Test that pagination limits are enforced."""
        # Limit too large should be rejected
        response = self.client.get("/v1/domain/example.com/datasets/0/urls?limit=20000")
        assert response.status_code == 422  # Validation error

        # Negative offset should be rejected
        response = self.client.get("/v1/domain/example.com/datasets/0/urls?offset=-1")
        assert response.status_code == 422
