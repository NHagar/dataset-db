"""
End-to-End Testing Script with Real Dataset

This script provides a complete workflow to test the dataset-db implementation
with your own real datasets:

1. Ingest a dataset (from HuggingFace or local file)
2. Build indexes
3. Start API server and run queries
4. Validate results

Usage:
    # With HuggingFace dataset
    python examples/e2e_test.py --dataset my_dataset --source huggingface

    # With local CSV/Parquet file
    python examples/e2e_test.py --dataset my_dataset --source local --file /path/to/urls.csv

    # Test with existing data (skip ingestion)
    python examples/e2e_test.py --skip-ingestion

    # Full automated test with validation
    python examples/e2e_test.py --dataset my_dataset --source huggingface --validate
"""

import argparse
import sys
import time
from pathlib import Path

import polars as pl

# Add src to path for direct execution
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dataset_db.ingestion import HuggingFaceLoader, IngestionProcessor
from dataset_db.storage import ParquetWriter
from dataset_db.index import IndexBuilder


def print_section(title):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(title.center(80))
    print("=" * 80)


def print_subsection(title):
    """Print a formatted subsection header."""
    print(f"\n--- {title} ---")


def ingest_huggingface(dataset_name: str, username: str = "nhagar", suffix: str = "_urls"):
    """Ingest data from HuggingFace dataset."""
    print_section("STEP 1: INGESTING FROM HUGGINGFACE")

    print(f"\nDataset: {username}/{dataset_name}{suffix}")
    print(f"Target: ./data/urls/")

    # Initialize components
    loader = HuggingFaceLoader(username=username, suffix=suffix)
    processor = IngestionProcessor()
    writer = ParquetWriter(base_path=Path("./data"))

    # Track stats
    batch_count = 0
    total_rows = 0
    start_time = time.time()

    try:
        print("\nStreaming batches...")
        for batch_df in loader.load(dataset_name, streaming=True):
            # Process batch
            normalized_df = processor.process_batch(batch_df, dataset_name)
            result = writer.write_batch(normalized_df)

            batch_count += 1
            total_rows += result["total_rows_processed"]

            # Print progress
            print(
                f"  Batch {batch_count}: {result['total_rows_processed']:,} rows | "
                f"buffered: {result['rows_buffered']:,}, flushed: {result['rows_flushed']:,}"
            )

            # Limit batches for testing (remove in production)
            if batch_count >= 10:  # Process first 10 batches for testing
                print("\n  (Stopping after 10 batches for testing)")
                break

        # Flush remaining data
        flush_result = writer.flush()
        writer_stats = writer.get_stats()

        elapsed = time.time() - start_time

        # Print summary
        print_subsection("Ingestion Summary")
        print(f"Total rows processed: {total_rows:,}")
        print(f"Rows written to disk: {writer_stats['rows_written']:,}")
        print(f"Final flush wrote: {flush_result['rows_written']:,} rows")
        print(f"Batches processed: {batch_count}")
        print(f"Time elapsed: {elapsed:.2f}s")
        print(f"Throughput: {total_rows / elapsed:,.0f} rows/sec")

        storage_stats = writer.get_storage_stats()
        print(f"\nStorage:")
        print(f"  Partitions: {storage_stats['total_partitions']}")
        print(f"  Files: {storage_stats['total_files']}")
        print(f"  Size: {storage_stats['total_size_bytes']:,} bytes")

        return True

    except Exception as e:
        print(f"\nError during ingestion: {e}")
        print("Make sure the dataset exists and you have access.")
        return False


def ingest_local(dataset_name: str, file_path: Path):
    """Ingest data from local file (CSV or Parquet)."""
    print_section("STEP 1: INGESTING FROM LOCAL FILE")

    print(f"\nFile: {file_path}")
    print(f"Dataset name: {dataset_name}")
    print(f"Target: ./data/urls/")

    # Check file exists
    if not file_path.exists():
        print(f"\nError: File not found: {file_path}")
        return False

    # Initialize components
    processor = IngestionProcessor()
    writer = ParquetWriter(base_path=Path("./data"))

    start_time = time.time()

    try:
        # Read file
        print("\nReading file...")
        if file_path.suffix == ".parquet":
            df = pl.read_parquet(file_path)
        elif file_path.suffix in [".csv", ".tsv"]:
            df = pl.read_csv(file_path)
        else:
            print(f"Error: Unsupported file type: {file_path.suffix}")
            return False

        print(f"Loaded {len(df):,} rows")

        # Check for URL column
        if "url" not in df.columns:
            print(f"\nError: File must have a 'url' column")
            print(f"Found columns: {df.columns}")
            return False

        # Process in batches
        batch_size = 100_000
        total_rows = 0
        batch_count = 0

        print(f"\nProcessing in batches of {batch_size:,}...")

        for i in range(0, len(df), batch_size):
            batch_df = df[i:i+batch_size]

            # Process batch
            normalized_df = processor.process_batch(batch_df, dataset_name)
            result = writer.write_batch(normalized_df)

            batch_count += 1
            total_rows += result["total_rows_processed"]

            print(
                f"  Batch {batch_count}: {result['total_rows_processed']:,} rows | "
                f"buffered: {result['rows_buffered']:,}, flushed: {result['rows_flushed']:,}"
            )

        # Flush remaining data
        flush_result = writer.flush()
        writer_stats = writer.get_stats()

        elapsed = time.time() - start_time

        # Print summary
        print_subsection("Ingestion Summary")
        print(f"Total rows processed: {total_rows:,}")
        print(f"Rows written to disk: {writer_stats['rows_written']:,}")
        print(f"Final flush wrote: {flush_result['rows_written']:,} rows")
        print(f"Batches processed: {batch_count}")
        print(f"Time elapsed: {elapsed:.2f}s")
        print(f"Throughput: {total_rows / elapsed:,.0f} rows/sec")

        storage_stats = writer.get_storage_stats()
        print(f"\nStorage:")
        print(f"  Partitions: {storage_stats['total_partitions']}")
        print(f"  Files: {storage_stats['total_files']}")
        print(f"  Size: {storage_stats['total_size_bytes']:,} bytes")

        return True

    except Exception as e:
        print(f"\nError during ingestion: {e}")
        import traceback
        traceback.print_exc()
        return False


def build_indexes(incremental: bool = False):
    """Build or rebuild indexes."""
    print_section("STEP 2: BUILDING INDEXES")

    base_path = Path("./data")
    builder = IndexBuilder(base_path=base_path)

    start_time = time.time()

    try:
        if incremental:
            print("\nRunning incremental build...")
            version = builder.build_incremental()
        else:
            print("\nRunning full build...")
            version = builder.build_all()

        elapsed = time.time() - start_time

        # Print summary
        print_subsection("Index Build Summary")
        print(f"Version: {version}")
        print(f"Time elapsed: {elapsed:.2f}s")

        # Load and inspect indexes
        from dataset_db.index import Manifest

        manifest = Manifest(base_path)  # Pass base_path, not base_path / "index"
        manifest.load()
        current = manifest.get_current_version()

        print(f"\nIndex files:")
        print(f"  Domain dictionary: {current.domains_txt}")
        print(f"  Domain MPHF: {current.domains_mphf}")
        print(f"  Membership index: {current.d2d_roar}")
        print(f"  File registry: {current.files_tsv}")

        # Show index stats
        from dataset_db.index import SimpleMPHF, MembershipIndex
        import zstandard as zstd

        # Load domain dictionary
        dict_path = base_path / current.domains_txt
        decompressor = zstd.ZstdDecompressor()
        compressed_data = dict_path.read_bytes()
        decompressed_data = decompressor.decompress(compressed_data)
        domains_text = decompressed_data.decode("utf-8")
        domains = [line for line in domains_text.split("\n") if line]

        print(f"\nDomains indexed: {len(domains):,}")

        # Show sample domains
        print(f"Sample domains:")
        for i, domain in enumerate(domains[:5]):
            print(f"  {i}: {domain}")

        # Load membership index
        membership = MembershipIndex(base_path)
        membership.load(base_path / current.d2d_roar, len(domains))
        print(f"\nDomain-to-dataset mappings: {len(membership.domain_bitmaps):,}")

        return True

    except Exception as e:
        print(f"\nError during index build: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_queries():
    """Test API queries."""
    print_section("STEP 3: TESTING QUERIES")

    base_path = Path("./data")

    # Check if indexes exist
    manifest_path = base_path / "index" / "manifest.json"
    if not manifest_path.exists():
        print("\nError: No indexes found! Run build_indexes() first.")
        return False

    try:
        # Load indexes
        from dataset_db.api import IndexLoader, QueryService

        print("\nLoading indexes...")
        loader = IndexLoader(base_path)
        loader.load()

        query_service = QueryService(loader)

        # Get sample domains
        from dataset_db.index import Manifest
        import zstandard as zstd

        manifest = Manifest(base_path)
        manifest.load()
        current = manifest.get_current_version()

        # Load domains
        dict_path = base_path / current.domains_txt
        decompressor = zstd.ZstdDecompressor()
        compressed_data = dict_path.read_bytes()
        decompressed_data = decompressor.decompress(compressed_data)
        domains_text = decompressed_data.decode("utf-8")
        domains = [line for line in domains_text.split("\n") if line]

        # Test with first few domains
        sample_domains = domains[:5]

        print_subsection("Testing Domain Queries")

        for domain in sample_domains:
            print(f"\nQuery: {domain}")

            # Get datasets for domain
            result = query_service.get_datasets_for_domain(domain)

            if result:
                print(f"  Domain ID: {result.domain_id}")
                print(f"  Datasets: {len(result.datasets)}")

                for ds in result.datasets:
                    print(f"    - Dataset {ds.dataset_id}: ~{ds.url_count_est} URLs")

                    # Get sample URLs from first dataset
                    if ds == result.datasets[0]:
                        print(f"\n    Sample URLs from dataset {ds.dataset_id}:")
                        urls_result = query_service.get_urls_for_domain_dataset(
                            domain, ds.dataset_id, offset=0, limit=5
                        )

                        for item in urls_result.items[:5]:
                            print(f"      - {item.url}")
            else:
                print(f"  No data found")

        print_subsection("Query Testing Complete")
        return True

    except Exception as e:
        print(f"\nError during query testing: {e}")
        import traceback
        traceback.print_exc()
        return False


def start_api_server():
    """Start the API server."""
    print_section("STEP 4: STARTING API SERVER")

    print("\nThe API server will start on http://0.0.0.0:8000")
    print("\nEndpoints:")
    print("  GET /v1/domain/{domain}")
    print("  GET /v1/domain/{domain}/datasets/{id}/urls")
    print("\nExample queries:")
    print('  curl http://localhost:8000/v1/domain/example.com')
    print('  curl "http://localhost:8000/v1/domain/example.com/datasets/1/urls?limit=10"')
    print("\nPress Ctrl+C to stop the server\n")

    try:
        import uvicorn
        from dataset_db.api.server import app

        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")

    except KeyboardInterrupt:
        print("\nServer stopped.")
    except Exception as e:
        print(f"\nError starting server: {e}")
        import traceback
        traceback.print_exc()


def validate_implementation():
    """Validate implementation correctness."""
    print_section("VALIDATION: CHECKING IMPLEMENTATION")

    base_path = Path("./data")

    print("\nRunning validation checks...")

    checks_passed = 0
    checks_total = 0

    # Check 1: Parquet files exist
    checks_total += 1
    print("\n1. Checking Parquet storage...")
    from dataset_db.storage import StorageLayout
    layout = StorageLayout(base_path)
    stats = layout.get_stats()

    if stats["total_files"] > 0:
        print(f"   ✓ Found {stats['total_files']} Parquet files")
        checks_passed += 1
    else:
        print(f"   ✗ No Parquet files found")

    # Check 2: Indexes exist
    checks_total += 1
    print("\n2. Checking indexes...")
    manifest_path = base_path / "index" / "manifest.json"

    if manifest_path.exists():
        from dataset_db.index import Manifest
        manifest = Manifest(base_path)
        manifest.load()
        current = manifest.get_current_version()
        print(f"   ✓ Found index version: {current.version}")
        checks_passed += 1
    else:
        print(f"   ✗ No index manifest found")

    # Check 3: Query service works
    checks_total += 1
    print("\n3. Checking query service...")

    try:
        from dataset_db.api import IndexLoader, QueryService
        import zstandard as zstd

        loader = IndexLoader(base_path)
        loader.load()

        query_service = QueryService(loader)

        # Get a sample domain
        dict_path = base_path / current.domains_txt
        decompressor = zstd.ZstdDecompressor()
        compressed_data = dict_path.read_bytes()
        decompressed_data = decompressor.decompress(compressed_data)
        domains_text = decompressed_data.decode("utf-8")
        domains = [line for line in domains_text.split("\n") if line]

        if len(domains) > 0:
            test_domain = domains[0]
            result = query_service.get_datasets_for_domain(test_domain)

            if result and len(result.datasets) > 0:
                print(f"   ✓ Query service working (tested {test_domain})")
                checks_passed += 1
            else:
                print(f"   ✗ Query returned no results for {test_domain}")
        else:
            print(f"   ✗ No domains to test")
    except Exception as e:
        print(f"   ✗ Query service error: {e}")

    # Check 4: URL reconstruction
    checks_total += 1
    print("\n4. Checking URL reconstruction...")

    try:
        # Read a sample Parquet file
        partitions = layout.list_partitions()
        if len(partitions) > 0:
            dataset_id, domain_prefix = partitions[0]
            writer = ParquetWriter(base_path=base_path)
            df = writer.read_partition(dataset_id, domain_prefix)

            if len(df) > 0:
                # Reconstruct first URL
                row = df[0]
                reconstructed = f"{row['scheme'][0]}://{row['host'][0]}{row['path_query'][0]}"
                print(f"   ✓ URL reconstruction works")
                print(f"     Example: {reconstructed}")
                checks_passed += 1
            else:
                print(f"   ✗ No data in partition")
        else:
            print(f"   ✗ No partitions to test")
    except Exception as e:
        print(f"   ✗ URL reconstruction error: {e}")

    # Print summary
    print_subsection("Validation Summary")
    print(f"\nPassed: {checks_passed}/{checks_total} checks")

    if checks_passed == checks_total:
        print("✓ All checks passed! Implementation is working correctly.")
        return True
    else:
        print("✗ Some checks failed. Review errors above.")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="End-to-end testing for dataset-db",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test with HuggingFace dataset
  python examples/e2e_test.py --dataset reddit --source huggingface

  # Test with local file
  python examples/e2e_test.py --dataset my_urls --source local --file urls.csv

  # Skip ingestion and test existing data
  python examples/e2e_test.py --skip-ingestion

  # Run full test with validation
  python examples/e2e_test.py --dataset reddit --source huggingface --validate

  # Start API server only
  python examples/e2e_test.py --server-only
        """
    )

    parser.add_argument("--dataset", help="Dataset name")
    parser.add_argument(
        "--source",
        choices=["huggingface", "local"],
        help="Data source (huggingface or local)"
    )
    parser.add_argument("--file", type=Path, help="Local file path (for --source local)")
    parser.add_argument(
        "--username",
        default="nhagar",
        help="HuggingFace username (default: nhagar)"
    )
    parser.add_argument(
        "--suffix",
        default="_urls",
        help="HuggingFace dataset suffix (default: _urls)"
    )
    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Skip ingestion, use existing data"
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Use incremental index build"
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run validation checks"
    )
    parser.add_argument(
        "--server-only",
        action="store_true",
        help="Only start the API server"
    )

    args = parser.parse_args()

    # Server-only mode
    if args.server_only:
        start_api_server()
        return

    # Validate arguments
    if not args.skip_ingestion and not args.dataset:
        parser.error("--dataset is required unless --skip-ingestion is used")

    if args.source == "local" and not args.file:
        parser.error("--file is required when --source is local")

    # Run workflow
    success = True

    # Step 1: Ingestion
    if not args.skip_ingestion:
        if args.source == "huggingface":
            success = ingest_huggingface(
                args.dataset,
                username=args.username,
                suffix=args.suffix
            )
        elif args.source == "local":
            success = ingest_local(args.dataset, args.file)

        if not success:
            print("\n✗ Ingestion failed. Exiting.")
            sys.exit(1)
    else:
        print_section("STEP 1: SKIPPING INGESTION (using existing data)")

    # Step 2: Build indexes
    if success:
        success = build_indexes(incremental=args.incremental)

        if not success:
            print("\n✗ Index building failed. Exiting.")
            sys.exit(1)

    # Step 3: Test queries
    if success:
        success = test_queries()

        if not success:
            print("\n✗ Query testing failed. Exiting.")
            sys.exit(1)

    # Step 4: Validation (optional)
    if success and args.validate:
        success = validate_implementation()

    # Step 5: Offer to start server
    if success:
        print_section("TESTING COMPLETE")
        print("\n✓ All steps completed successfully!")

        response = input("\nStart API server? (y/n): ").strip().lower()
        if response == "y":
            start_api_server()
    else:
        print("\n✗ Testing failed. Please review errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
