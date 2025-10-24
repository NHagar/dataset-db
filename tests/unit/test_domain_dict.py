"""Tests for domain dictionary builder."""


import polars as pl
import pytest

from dataset_db.index import DomainDictionary
from dataset_db.storage import ParquetWriter


@pytest.fixture
def temp_data_path(tmp_path):
    """Create temporary data directory."""
    return tmp_path / "data"


@pytest.fixture
def sample_parquet_data(temp_data_path):
    """Create sample Parquet files for testing."""
    writer = ParquetWriter(base_path=temp_data_path)

    # Create test data with multiple domains
    df1 = pl.DataFrame(
        {
            "domain_id": [1, 2, 3],
            "url_id": [100, 200, 300],
            "scheme": ["https", "https", "http"],
            "host": ["example.com", "test.com", "demo.org"],
            "path_query": ["/", "/page", "/path"],
            "domain": ["example.com", "test.com", "demo.org"],
        }
    )

    df2 = pl.DataFrame(
        {
            "domain_id": [2, 4],
            "url_id": [200, 400],
            "scheme": ["https", "https"],
            "host": ["test.com", "another.com"],
            "path_query": ["/other", "/"],
            "domain": ["test.com", "another.com"],
        }
    )

    # Write to different partitions
    writer._write_partition(df1, dataset_id=0, domain_prefix="aa")
    writer._write_partition(df2, dataset_id=1, domain_prefix="bb")

    return temp_data_path


def test_extract_unique_domains(sample_parquet_data):
    """Test extracting unique domains from Parquet files."""
    domain_dict = DomainDictionary(sample_parquet_data)

    domains = domain_dict.extract_unique_domains()

    # Should have 4 unique domains, sorted
    assert len(domains) == 4
    assert domains == ["another.com", "demo.org", "example.com", "test.com"]


def test_extract_unique_domains_specific_datasets(sample_parquet_data):
    """Test extracting unique domains for specific datasets."""
    domain_dict = DomainDictionary(sample_parquet_data)

    # Only dataset 0
    domains = domain_dict.extract_unique_domains(dataset_ids=[0])
    assert len(domains) == 3
    assert "another.com" not in domains


def test_write_and_read_domain_dict(temp_data_path):
    """Test writing and reading domain dictionary."""
    domain_dict = DomainDictionary(temp_data_path)

    test_domains = ["apple.com", "banana.com", "cherry.com"]
    version = "2025-01-01T00:00:00Z"

    # Write
    output_path = domain_dict.write_domain_dict(test_domains, version)
    assert output_path.exists()
    assert output_path.name == "domains.txt.zst"

    # Read back
    loaded_domains = domain_dict.read_domain_dict(version)
    assert loaded_domains == test_domains


def test_iter_domains(temp_data_path):
    """Test iterating over domains with IDs."""
    domain_dict = DomainDictionary(temp_data_path)

    test_domains = ["alpha.com", "beta.com", "gamma.com"]
    version = "2025-01-01T00:00:00Z"

    domain_dict.write_domain_dict(test_domains, version)

    # Iterate
    result = list(domain_dict.iter_domains(version))

    assert len(result) == 3
    assert result[0] == (0, "alpha.com")
    assert result[1] == (1, "beta.com")
    assert result[2] == (2, "gamma.com")


def test_build_full_pipeline(sample_parquet_data):
    """Test full build pipeline."""
    domain_dict = DomainDictionary(sample_parquet_data)
    version = "2025-01-01T00:00:00Z"

    # Build
    output_path = domain_dict.build(version)

    assert output_path.exists()

    # Verify
    domains = domain_dict.read_domain_dict(version)
    assert len(domains) == 4
    assert domains == sorted(domains)


def test_compression_ratio(temp_data_path):
    """Test that compression provides good ratio for repetitive data."""
    domain_dict = DomainDictionary(temp_data_path)

    # Create many similar domains
    test_domains = [f"subdomain{i}.example.com" for i in range(1000)]
    version = "2025-01-01T00:00:00Z"

    output_path = domain_dict.write_domain_dict(test_domains, version)

    # Calculate compression ratio
    uncompressed_size = sum(len(d) + 1 for d in test_domains)  # +1 for newline
    compressed_size = output_path.stat().st_size

    ratio = uncompressed_size / compressed_size

    # Should achieve at least 2x compression for repetitive domains
    assert ratio > 2.0


def test_empty_domains(temp_data_path):
    """Test handling empty domain list."""
    domain_dict = DomainDictionary(temp_data_path)
    version = "2025-01-01T00:00:00Z"

    # Write empty list
    output_path = domain_dict.write_domain_dict([], version)
    assert output_path.exists()

    # Read back
    domains = domain_dict.read_domain_dict(version)
    assert domains == []


def test_unicode_domains(temp_data_path):
    """Test handling of internationalized domain names."""
    domain_dict = DomainDictionary(temp_data_path)

    # Punycode domains
    test_domains = [
        "xn--80akhbyknj4f.com",  # пример.com (Russian)
        "xn--fsq.com",  # 例.com (Chinese)
        "example.com",
    ]
    version = "2025-01-01T00:00:00Z"

    domain_dict.write_domain_dict(test_domains, version)

    # Read back
    loaded_domains = domain_dict.read_domain_dict(version)
    assert loaded_domains == test_domains


def test_read_nonexistent_version(temp_data_path):
    """Test error handling for nonexistent version."""
    domain_dict = DomainDictionary(temp_data_path)

    with pytest.raises(FileNotFoundError):
        domain_dict.read_domain_dict("nonexistent")
