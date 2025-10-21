"""
Basic ingestion example.

Demonstrates how to use the URL normalizer and ingestion processor.
"""

import polars as pl

from dataset_db.ingestion import IngestionProcessor
from dataset_db.normalization import IDGenerator, URLNormalizer


def main():
    """Run basic ingestion example."""
    print("=" * 60)
    print("Dataset-DB: Basic Ingestion Example")
    print("=" * 60)

    # Create normalizer and ID generator
    normalizer = URLNormalizer()
    id_generator = IDGenerator()
    processor = IngestionProcessor(normalizer=normalizer, id_generator=id_generator)

    # Example 1: Normalize a single URL
    print("\n1. Single URL Normalization")
    print("-" * 60)

    raw_url = "HTTPS://WWW.Example.COM:443/path/../clean?z=1&a=2#fragment"
    print(f"Raw URL: {raw_url}")

    normalized = normalizer.normalize(raw_url)
    print(f"\nNormalized URL: {normalized.to_url()}")
    print(f"Domain (eTLD+1): {normalized.domain}")
    print(f"Scheme: {normalized.scheme}")
    print(f"Host: {normalized.host}")
    print(f"Path: {normalized.path}")
    print(f"Query: {normalized.query}")

    # Example 2: Process a batch of URLs
    print("\n\n2. Batch Processing")
    print("-" * 60)

    sample_data = pl.DataFrame({
        "url": [
            "https://www.example.com/page1",
            "https://subdomain.example.com/page2?param=value",
            "http://example.org:8080/path/to/resource",
            "https://www.example.co.uk/british-site",
            "HTTPS://SHOUTY.COM/PATH",
        ],
        "domain": ["example.com", "example.com", "example.org", "example.co.uk", "shouty.com"],
    })

    print(f"Input: {len(sample_data)} URLs")
    print("\nSample URLs:")
    for url in sample_data["url"][:3]:
        print(f"  - {url}")

    # Process the batch
    result = processor.process_batch(sample_data, "example_dataset")

    print(f"\nOutput: {len(result)} normalized records")
    print("\nOutput Schema:")
    for col, dtype in result.schema.items():
        print(f"  {col}: {dtype}")

    # Show normalized results
    print("\nNormalized Records (first 3):")
    print(result.select([
        "domain",
        "scheme",
        "host",
        "path_query",
        "domain_prefix",
    ]).head(3))

    # Example 3: Multiple datasets
    print("\n\n3. Multiple Datasets")
    print("-" * 60)

    dataset1 = pl.DataFrame({"url": ["https://example.com/dataset1"]})
    dataset2 = pl.DataFrame({"url": ["https://example.org/dataset2"]})

    processor.process_batch(dataset1, "dataset_A")
    processor.process_batch(dataset2, "dataset_B")

    stats = processor.get_stats()
    print(f"Datasets processed: {stats['datasets_processed']}")
    print("Dataset registry:")
    for name, ds_id in stats["datasets"].items():
        print(f"  {name}: ID={ds_id}")

    # Example 4: ID generation
    print("\n\n4. ID Generation")
    print("-" * 60)

    url1 = "https://example.com/page"
    url2 = "https://example.com/page"  # Same URL
    url3 = "https://example.com/different"

    url_id1 = id_generator.get_url_id(url1)
    url_id2 = id_generator.get_url_id(url2)
    url_id3 = id_generator.get_url_id(url3)

    print(f"URL: {url1}")
    print(f"  URL ID: {url_id1}")
    print(f"\nURL: {url2} (same as above)")
    print(f"  URL ID: {url_id2}")
    print(f"  IDs match: {url_id1 == url_id2}")
    print(f"\nURL: {url3}")
    print(f"  URL ID: {url_id3}")
    print(f"  IDs different: {url_id1 != url_id3}")

    # Domain IDs
    domain1 = "example.com"
    domain2 = "www.example.com"  # Different, but normalizer extracts same eTLD+1

    norm1 = normalizer.normalize(f"https://{domain1}/")
    norm2 = normalizer.normalize(f"https://{domain2}/")

    domain_id1 = id_generator.get_domain_id(norm1.domain)
    domain_id2 = id_generator.get_domain_id(norm2.domain)

    print(f"\nDomain: {domain1}")
    print(f"  eTLD+1: {norm1.domain}")
    print(f"  Domain ID: {domain_id1}")
    print(f"\nDomain: {domain2}")
    print(f"  eTLD+1: {norm2.domain}")
    print(f"  Domain ID: {domain_id2}")
    print(f"  IDs match (same eTLD+1): {domain_id1 == domain_id2}")

    # Domain prefixes for partitioning
    prefix = id_generator.get_domain_prefix("example.com", prefix_chars=2)
    print(f"\nDomain prefix for 'example.com': {prefix}")

    # Example 5: URL Reconstruction
    print("\n\n5. URL Reconstruction")
    print("-" * 60)
    print("Note: raw_url is not stored to save disk space (3x reduction)")
    print("URLs can be reconstructed from components:\n")

    test_url = "HTTPS://Example.COM:443/path/../clean?z=1&a=2#fragment"
    print(f"Original URL: {test_url}")

    # Process it
    test_df = pl.DataFrame({"url": [test_url]})
    result = processor.process_batch(test_df, "reconstruction_test")
    row = result.row(0, named=True)

    # Reconstruct
    reconstructed = processor.reconstruct_url(
        row["scheme"], row["host"], row["path_query"]
    )

    print(f"Normalized & Reconstructed: {reconstructed}")
    print("\nComponents stored:")
    print(f"  scheme: {row['scheme']}")
    print(f"  host: {row['host']}")
    print(f"  path_query: {row['path_query']}")
    print(f"\nReconstruction: {row['scheme']}://{row['host']}{row['path_query']}")

    print("\n" + "=" * 60)
    print("Example complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
