"""Tests for MPHF (Minimal Perfect Hash Function)."""


import pytest

from dataset_db.index import SimpleMPHF


@pytest.fixture
def temp_path(tmp_path):
    """Create temporary directory."""
    return tmp_path


def test_build_and_lookup():
    """Test basic build and lookup."""
    mphf = SimpleMPHF()

    domains = ["apple.com", "banana.com", "cherry.com"]
    mphf.build(domains)

    # Lookup existing domains
    assert mphf.lookup("apple.com") == 0
    assert mphf.lookup("banana.com") == 1
    assert mphf.lookup("cherry.com") == 2


def test_lookup_nonexistent():
    """Test lookup of domain not in MPHF."""
    mphf = SimpleMPHF()

    domains = ["example.com"]
    mphf.build(domains)

    assert mphf.lookup("nonexistent.com") is None


def test_large_domain_set():
    """Test with larger domain set."""
    mphf = SimpleMPHF()

    # Create 10,000 domains
    domains = [f"domain{i}.example.com" for i in range(10000)]
    mphf.build(domains)

    # Verify all lookups work
    for i in range(0, 10000, 100):  # Sample every 100th
        domain = f"domain{i}.example.com"
        assert mphf.lookup(domain) == i


def test_save_and_load(temp_path):
    """Test saving and loading MPHF."""
    mphf = SimpleMPHF()

    domains = ["test1.com", "test2.com", "test3.com"]
    mphf.build(domains)

    # Save
    save_path = temp_path / "test.mphf"
    mphf.save(save_path)

    assert save_path.exists()

    # Load into new instance
    mphf2 = SimpleMPHF()
    mphf2.load(save_path)

    # Verify lookups work
    assert mphf2.lookup("test1.com") == 0
    assert mphf2.lookup("test2.com") == 1
    assert mphf2.lookup("test3.com") == 2


def test_compression(temp_path):
    """Test that MPHF file is compressed."""
    mphf = SimpleMPHF()

    domains = [f"subdomain{i}.verylongdomainname.com" for i in range(1000)]
    mphf.build(domains)

    save_path = temp_path / "test.mphf"
    mphf.save(save_path, compression_level=6)

    # File should be relatively small due to compression
    file_size = save_path.stat().st_size

    # Rough estimate: should be less than 50 bytes per domain with compression
    assert file_size < len(domains) * 50


def test_unicode_domains(temp_path):
    """Test MPHF with unicode (punycode) domains."""
    mphf = SimpleMPHF()

    domains = [
        "example.com",
        "xn--80akhbyknj4f.com",  # пример.com
        "xn--fsq.com",  # 例.com
    ]
    mphf.build(domains)

    # Save and load
    save_path = temp_path / "unicode.mphf"
    mphf.save(save_path)

    mphf2 = SimpleMPHF()
    mphf2.load(save_path)

    # Verify all lookups
    assert mphf2.lookup("example.com") == 0
    assert mphf2.lookup("xn--80akhbyknj4f.com") == 1
    assert mphf2.lookup("xn--fsq.com") == 2


def test_hash_collision_handling():
    """Test that hash collisions are handled correctly."""
    mphf = SimpleMPHF()

    # Use a large set to increase collision probability
    # (though xxh3_64 is very collision-resistant)
    domains = [f"domain{i}.com" for i in range(100000)]
    mphf.build(domains)

    # All lookups should still work correctly
    for i in range(0, 100000, 1000):
        domain = f"domain{i}.com"
        result = mphf.lookup(domain)
        assert result == i


def test_empty_domain_set():
    """Test building MPHF with empty domain set."""
    mphf = SimpleMPHF()

    mphf.build([])

    assert mphf.lookup("anything.com") is None


def test_single_domain():
    """Test MPHF with single domain."""
    mphf = SimpleMPHF()

    mphf.build(["single.com"])

    assert mphf.lookup("single.com") == 0
    assert mphf.lookup("other.com") is None


def test_load_invalid_file(temp_path):
    """Test error handling for invalid MPHF file."""
    invalid_path = temp_path / "invalid.mphf"
    invalid_path.write_bytes(b"not a valid mphf file")

    mphf = SimpleMPHF()

    with pytest.raises(Exception):  # Should raise ValueError or similar
        mphf.load(invalid_path)


def test_load_nonexistent_file(temp_path):
    """Test error handling for nonexistent file."""
    mphf = SimpleMPHF()

    with pytest.raises(FileNotFoundError):
        mphf.load(temp_path / "nonexistent.mphf")


def test_build_from_dict(temp_path):
    """Test static build_from_dict method."""
    domains = ["a.com", "b.com", "c.com"]
    version = "2025-01-01T00:00:00Z"

    output_path = SimpleMPHF.build_from_dict(domains, version, temp_path)

    assert output_path.exists()

    # Load and verify
    mphf = SimpleMPHF()
    mphf.load(output_path)

    assert mphf.lookup("a.com") == 0
    assert mphf.lookup("b.com") == 1
    assert mphf.lookup("c.com") == 2
