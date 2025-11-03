"""
Data Inspector

Quick utility to inspect existing data in ./data/

Usage:
    python examples/inspect_data.py                    # Show summary
    python examples/inspect_data.py --details          # Show detailed info
    python examples/inspect_data.py --domain example.com  # Query specific domain
"""

import argparse
import sys
from pathlib import Path


# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def inspect_storage():
    """Inspect Parquet storage."""
    print("=" * 80)
    print("PARQUET STORAGE")
    print("=" * 80)

    from dataset_db.storage import StorageLayout, ParquetWriter

    base_path = Path("./data")
    layout = StorageLayout(base_path)
    stats = layout.get_stats()

    if stats["total_files"] == 0:
        print("\nNo data found in ./data/")
        print("Run ingestion first: python examples/e2e_test.py --dataset <name> --source <source>")
        return False

    print("\nStorage Statistics:")
    print(f"  Total files: {stats['total_files']}")
    print(f"  Total partitions: {stats['total_partitions']}")
    print(f"  Total size: {stats['total_size_bytes']:,} bytes ({stats['total_size_bytes'] / 1024 / 1024:.2f} MB)")

    # Show datasets
    print("\nDatasets:")
    for ds in stats["datasets"]:
        print(f"  Dataset {ds['dataset_id']}: {ds['partitions']} partitions, {ds['files']} files")

    # Show sample data
    print("\nSample Data:")
    writer = ParquetWriter(base_path=base_path)
    partitions = layout.list_partitions()

    for dataset_id, domain_prefix in partitions[:3]:
        print(f"\n  Dataset {dataset_id}, Prefix {domain_prefix}:")
        df = writer.read_partition(dataset_id, domain_prefix)

        print(f"    Rows: {len(df):,}")
        print(f"    Unique domains: {df['domain'].n_unique()}")

        # Show sample URLs
        for i, row in enumerate(df.head(3).iter_rows(named=True)):
            url = f"{row['scheme']}://{row['host']}{row['path_query']}"
            print(f"      {i+1}. {url}")

    return True


def inspect_indexes():
    """Inspect index files."""
    print("\n" + "=" * 80)
    print("INDEXES")
    print("=" * 80)

    base_path = Path("./data")
    manifest_path = base_path / "index" / "manifest.json"

    if not manifest_path.exists():
        print("\nNo indexes found.")
        print("Run index build: python examples/e2e_test.py --skip-ingestion")
        return False

    from dataset_db.index import Manifest, MembershipIndex
    import zstandard as zstd

    # Load manifest
    manifest = Manifest(base_path)
    manifest.load()
    current = manifest.get_current_version()

    print(f"\nCurrent Version: {current.version}")
    print("\nIndex Files:")
    print(f"  Domain dictionary: {current.domains_txt}")
    print(f"  Domain MPHF: {current.domains_mphf}")
    print(f"  Membership index: {current.d2d_roar}")
    print(f"  File registry: {current.files_tsv}")

    # Load domain dictionary
    dict_path = base_path / current.domains_txt
    decompressor = zstd.ZstdDecompressor()
    compressed_data = dict_path.read_bytes()
    decompressed_data = decompressor.decompress(compressed_data)
    domains_text = decompressed_data.decode("utf-8")
    domains = [line for line in domains_text.split("\n") if line]

    print("\nDomain Statistics:")
    print(f"  Total domains: {len(domains):,}")

    # Show sample domains
    print("\nSample Domains (first 10):")
    for i, domain in enumerate(domains[:10]):
        print(f"  {i+1}. {domain}")

    # Load membership index
    membership = MembershipIndex(base_path)
    membership.load(base_path / current.d2d_roar, len(domains))

    print("\nMembership Statistics:")
    print(f"  Domain-to-dataset mappings: {len(membership.domain_bitmaps):,}")

    # Show some membership info
    print("\nSample Memberships:")
    for i, (domain_id, bitmap) in enumerate(list(membership.domain_bitmaps.items())[:5]):
        dataset_ids = list(bitmap)
        if i < len(domains):
            domain_str = domains[i]
        else:
            domain_str = f"Domain ID {domain_id}"
        print(f"  {domain_str}: datasets {dataset_ids}")

    return True


def query_domain(domain: str):
    """Query a specific domain."""
    print("=" * 80)
    print(f"QUERYING: {domain}")
    print("=" * 80)

    base_path = Path("./data")

    # Check indexes exist
    manifest_path = base_path / "index" / "manifest.json"
    if not manifest_path.exists():
        print("\nNo indexes found. Run index build first.")
        return False

    try:
        from dataset_db.api import IndexLoader, QueryService

        # Load indexes
        print("\nLoading indexes...")
        loader = IndexLoader(base_path)
        loader.load()

        query_service = QueryService(loader)

        # Query domain
        print(f"Querying domain: {domain}")
        result = query_service.get_datasets_for_domain(domain)

        if not result:
            print(f"\nNo data found for domain: {domain}")
            return True

        print(f"\nDomain ID: {result.domain_id}")
        print(f"Found in {len(result.datasets)} dataset(s):")

        for ds in result.datasets:
            print(f"\n  Dataset {ds.dataset_id}:")
            print(f"    Estimated URLs: {ds.url_count_est or 'unknown'}")

            # Get sample URLs
            print("    Sample URLs:")
            urls_result = query_service.get_urls_for_domain_dataset(
                domain, ds.dataset_id, offset=0, limit=10
            )

            for i, item in enumerate(urls_result.items[:10]):
                print(f"      {i+1}. {item.url}")

        return True

    except Exception as e:
        print(f"\nError querying domain: {e}")
        import traceback
        traceback.print_exc()
        return False


def show_dataset_registry():
    """Show dataset registry."""
    print("\n" + "=" * 80)
    print("DATASET REGISTRY")
    print("=" * 80)

    try:
        from dataset_db.ingestion.processor import DatasetRegistry

        registry = DatasetRegistry()
        registry_dict = registry.to_dict()

        if not registry_dict:
            print("\nNo datasets registered yet.")
            return

        print(f"\nRegistered Datasets: {len(registry_dict)}")
        for name, dataset_id in sorted(registry_dict.items(), key=lambda x: x[1]):
            print(f"  ID {dataset_id}: {name}")
    except Exception as e:
        print(f"\nCould not load dataset registry: {e}")
        print("This is optional and doesn't affect functionality.")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Inspect data and indexes in ./data/",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--details",
        action="store_true",
        help="Show detailed information"
    )
    parser.add_argument(
        "--domain",
        help="Query a specific domain"
    )

    args = parser.parse_args()

    # Query specific domain
    if args.domain:
        query_domain(args.domain)
        return

    # Otherwise show overview
    has_storage = inspect_storage()

    if has_storage:
        has_indexes = inspect_indexes()

        if has_indexes:
            show_dataset_registry()

    # Show next steps
    print("\n" + "=" * 80)
    print("NEXT STEPS")
    print("=" * 80)

    if not has_storage:
        print("\n1. Ingest some data:")
        print("   python examples/e2e_test.py --dataset <name> --source huggingface")
        print("   or")
        print("   python examples/e2e_test.py --dataset <name> --source local --file <path>")
    elif not has_indexes:
        print("\n1. Build indexes:")
        print("   python examples/e2e_test.py --skip-ingestion")
    else:
        print("\n✓ Data and indexes are ready!")
        print("\nQuery a domain:")
        print("   python examples/inspect_data.py --domain example.com")
        print("\nOr start the API server:")
        print("   python examples/e2e_test.py --server-only")


if __name__ == "__main__":
    main()
