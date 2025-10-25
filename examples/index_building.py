"""
Example: Index Building

Demonstrates building indexes for the dataset-db system:
1. Build domain dictionary
2. Build MPHF for fast domain lookups
3. Build membership index (domain → datasets)
4. Build file registry
5. Build postings index (domain, dataset → row groups)
6. Publish to manifest

Prerequisites:
- Run parquet_ingestion.py first to create Parquet files
"""

import logging
from pathlib import Path

from dataset_db.index import IndexBuilder

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def example_1_basic_index_building():
    """Example 1: Build indexes from existing Parquet files."""
    print("\n" + "=" * 80)
    print("Example 1: Basic Index Building")
    print("=" * 80 + "\n")

    base_path = Path("./data")

    # Check if we have Parquet files
    urls_dir = base_path / "urls"
    if not urls_dir.exists() or not list(urls_dir.rglob("*.parquet")):
        print("No Parquet files found. Please run parquet_ingestion.py first.")
        return

    # Initialize builder
    builder = IndexBuilder(
        base_path=base_path,
        num_postings_shards=16,  # Use fewer shards for demo
        compression_level=6,
    )

    # Build all indexes
    print("Building indexes (this may take a few moments)...")
    version = builder.build_all()

    print("\nIndexes built successfully!")
    print(f"Version: {version}")

    # Get statistics
    stats = builder.get_stats(version)
    print("\nIndex Statistics:")
    print(f"  Domains: {stats['num_domains']:,}")
    print(f"  Files: {stats['num_files']:,}")
    print(f"  Domain-Dataset pairs: {stats['num_domain_dataset_pairs']:,}")
    print(f"  Postings shards: {stats['num_postings_shards']:,}")


def example_2_query_indexes():
    """Example 2: Query the built indexes."""
    print("\n" + "=" * 80)
    print("Example 2: Query Indexes")
    print("=" * 80 + "\n")

    base_path = Path("./data")

    # Load manifest
    from dataset_db.index import Manifest

    manifest = Manifest(base_path)
    manifest.load()

    current = manifest.get_current_version()
    if not current:
        print("No current version found. Build indexes first.")
        return

    print(f"Current version: {current.version}")
    print(f"Created at: {current.created_at}")

    # Load indexes
    from dataset_db.index import DomainDictionary, MembershipIndex, SimpleMPHF

    domain_dict = DomainDictionary(base_path)
    domains = domain_dict.read_domain_dict(current.version)

    mphf = SimpleMPHF()
    mphf_path = base_path / current.domains_mphf
    mphf.load(mphf_path)

    membership = MembershipIndex(base_path)
    membership_path = base_path / current.d2d_roar
    membership.load(membership_path, len(domains))

    # Example queries
    print(f"\nTotal domains in index: {len(domains):,}")
    print("\nFirst 10 domains:")
    for i, domain in enumerate(domains[:10]):
        print(f"  {i}: {domain}")

    # Look up a domain
    if domains:
        test_domain = domains[0]
        print(f"\nLooking up domain: {test_domain}")

        domain_id = mphf.lookup(test_domain)
        print(f"  Domain ID: {domain_id}")

        if domain_id is not None:
            datasets = membership.get_datasets(domain_id)
            print(f"  Found in {len(datasets)} dataset(s): {datasets}")


def example_3_incremental_build():
    """Example 3: Incremental index building for specific datasets."""
    print("\n" + "=" * 80)
    print("Example 3: Incremental Index Building")
    print("=" * 80 + "\n")

    base_path = Path("./data")

    # Build indexes for specific datasets only
    builder = IndexBuilder(base_path=base_path, num_postings_shards=16)

    # Suppose we only want to index dataset 0
    print("Building indexes for dataset 0 only...")
    version = builder.build_incremental(dataset_ids=[0])

    print("\nIncremental build complete!")
    print(f"Version: {version}")

    # Get statistics
    stats = builder.get_stats(version)
    print("\nIndex Statistics:")
    print(f"  Domains: {stats['num_domains']:,}")
    print(f"  Files: {stats['num_files']:,}")


def example_4_manifest_management():
    """Example 4: Manage multiple index versions."""
    print("\n" + "=" * 80)
    print("Example 4: Manifest Management")
    print("=" * 80 + "\n")

    from dataset_db.index import Manifest

    base_path = Path("./data")
    manifest = Manifest(base_path)
    manifest.load()

    print("All versions:")
    for version in manifest.list_versions():
        v = manifest.get_version(version)
        marker = " (current)" if version == manifest.current_version else ""
        print(f"  {version}{marker}")
        print(f"    Created: {v.created_at}")

    # Cleanup old versions (keeping last 3)
    if len(manifest.versions) > 3:
        print("\nCleaning up old versions (keeping last 3)...")
        removed = manifest.cleanup_old_versions(keep_last_n=3)
        print(f"Removed versions: {removed}")
        manifest.save()


if __name__ == "__main__":
    print("Dataset-DB Index Building Examples")
    print("=" * 80)

    # Run examples
    example_1_basic_index_building()
    example_2_query_indexes()
    # example_3_incremental_build()  # Uncomment to test incremental builds
    example_4_manifest_management()

    print("\n" + "=" * 80)
    print("All examples completed!")
    print("=" * 80)
