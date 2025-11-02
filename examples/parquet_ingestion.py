"""
End-to-end Parquet ingestion example.

Demonstrates the complete pipeline:
1. Load dataset from HuggingFace
2. Normalize URLs through IngestionProcessor
3. Write to partitioned Parquet storage
"""

import sys
from pathlib import Path

import polars as pl

# Add src to path for direct execution
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dataset_db.ingestion import HuggingFaceLoader, IngestionProcessor
from dataset_db.normalization import IDGenerator, URLNormalizer
from dataset_db.storage import ParquetWriter


def example_basic_write():
    """Basic example: write a small batch of URLs."""
    print("=" * 70)
    print("Example 1: Basic Parquet Write")
    print("=" * 70)

    # Initialize components
    normalizer = URLNormalizer()
    id_generator = IDGenerator()
    processor = IngestionProcessor(normalizer, id_generator)
    writer = ParquetWriter(base_path=Path("./data"))

    # Sample URLs
    sample_data = pl.DataFrame(
        {
            "url": [
                "https://example.com/path1?a=1",
                "https://example.com/path2?b=2",
                "https://another.org/test",
                "https://subdomain.example.com/page",
                "http://Example.COM/Path",  # Will normalize to lowercase
            ]
        }
    )

    print(f"\nInput: {len(sample_data)} URLs")
    print(sample_data)

    # Process through normalization
    normalized_df = processor.process_batch(sample_data, "example_dataset")

    print(f"\nNormalized: {len(normalized_df)} records")
    print(normalized_df)

    # Write to Parquet
    result = writer.write_batch(normalized_df)

    # Flush any remaining buffered data
    flush_result = writer.flush()

    print(f"\nBatch result: processed={result['total_rows_processed']}, "
          f"buffered={result['rows_buffered']}, flushed={result['rows_flushed']}")
    print(f"Flush result: {flush_result['rows_written']} rows written")
    print(f"Writer stats: {writer.get_stats()}")
    print(f"Storage stats: {writer.get_storage_stats()}")


def example_multiple_datasets():
    """Example: process multiple datasets into partitioned storage."""
    print("\n" + "=" * 70)
    print("Example 2: Multiple Datasets with Partitioning")
    print("=" * 70)

    # Initialize components
    normalizer = URLNormalizer()
    id_generator = IDGenerator()
    processor = IngestionProcessor(normalizer, id_generator)
    writer = ParquetWriter(base_path=Path("./data"))

    # Simulate multiple datasets
    datasets = {
        "news_sites": [
            "https://nytimes.com/article1",
            "https://nytimes.com/article2",
            "https://bbc.co.uk/news/story1",
        ],
        "tech_sites": [
            "https://github.com/user/repo",
            "https://stackoverflow.com/questions/123",
            "https://docs.python.org/3/library/",
        ],
        "social_media": [
            "https://twitter.com/user/status/123",
            "https://reddit.com/r/programming",
            "https://linkedin.com/in/profile",
        ],
    }

    total_rows = 0

    for dataset_name, urls in datasets.items():
        print(f"\n--- Processing: {dataset_name} ---")

        # Create DataFrame
        df = pl.DataFrame({"url": urls})

        # Normalize
        normalized_df = processor.process_batch(df, dataset_name)

        print(f"Dataset ID: {normalized_df['dataset_id'][0]}")
        print(f"Unique domains: {normalized_df['domain'].n_unique()}")
        print(f"Domain prefixes: {sorted(normalized_df['domain_prefix'].unique())}")

        # Write
        result = writer.write_batch(normalized_df)

        total_rows += result["total_rows_processed"]

        print(f"Processed {result['total_rows_processed']} rows: "
              f"buffered={result['rows_buffered']}, flushed={result['rows_flushed']} "
              f"({result['files_written']} files)")

    # Flush any remaining buffered data
    flush_result = writer.flush()
    print(f"\nFinal flush: {flush_result['rows_written']} rows to {flush_result['files_written']} files")

    print("\n--- Summary ---")
    writer_stats = writer.get_stats()
    print(f"Total rows processed: {total_rows}")
    print(f"Rows written to disk: {writer_stats['rows_written']}")
    print(f"Total files created: {writer_stats['files_created']}")
    print("\nStorage stats:")

    storage_stats = writer.get_storage_stats()
    print(f"  Partitions: {storage_stats['total_partitions']}")
    print(f"  Files: {storage_stats['total_files']}")
    print(f"  Size: {storage_stats['total_size_bytes']:,} bytes")

    print("\nDatasets:")
    for ds in storage_stats["datasets"]:
        print(
            f"  Dataset {ds['dataset_id']}: {ds['partitions']} partitions, {ds['files']} files"
        )


def example_read_back():
    """Example: read data back from Parquet."""
    print("\n" + "=" * 70)
    print("Example 3: Read Data Back from Parquet")
    print("=" * 70)

    writer = ParquetWriter(base_path=Path("./data"))
    storage_stats = writer.get_storage_stats()

    if storage_stats["total_files"] == 0:
        print("No data written yet. Run previous examples first.")
        return

    print(f"Found {storage_stats['total_partitions']} partitions")

    # Get all partitions
    from dataset_db.storage.layout import StorageLayout

    layout = StorageLayout(Path("./data"))
    partitions = layout.list_partitions()

    # Read first few partitions
    for dataset_id, domain_prefix in partitions[:3]:
        print(f"\n--- Dataset {dataset_id}, Domain Prefix {domain_prefix} ---")

        try:
            df = writer.read_partition(dataset_id, domain_prefix)

            print(f"Rows: {len(df)}")
            print(f"Columns: {df.columns}")

            # Show sample data
            print("\nSample data:")
            print(df.head(3))

            # Reconstruct URLs
            print("\nReconstructed URLs:")
            for row in df.head(3).iter_rows(named=True):
                url = f"{row['scheme']}://{row['host']}{row['path_query']}"
                print(f"  {url}")

        except FileNotFoundError as e:
            print(f"Error: {e}")


def example_compression_stats():
    """Example: show compression effectiveness."""
    print("\n" + "=" * 70)
    print("Example 4: Compression Statistics")
    print("=" * 70)

    from dataset_db.storage.layout import StorageLayout

    layout = StorageLayout(Path("./data"))
    storage_stats = layout.get_stats()

    if storage_stats["total_files"] == 0:
        print("No data written yet. Run previous examples first.")
        return

    print(f"Total storage used: {storage_stats['total_size_bytes']:,} bytes")
    print(f"Total files: {storage_stats['total_files']}")

    if storage_stats["total_files"] > 0:
        avg_file_size = storage_stats["total_size_bytes"] / storage_stats["total_files"]
        print(f"Average file size: {avg_file_size:,.0f} bytes")

    # Show partition-level details
    print("\nPartition details:")
    partitions = layout.list_partitions()

    for dataset_id, domain_prefix in partitions[:5]:  # Show first 5
        files = layout.list_parquet_files(dataset_id, domain_prefix)
        total_size = sum(f.stat().st_size for f in files)

        print(f"  Dataset {dataset_id}, Prefix {domain_prefix}:")
        print(f"    Files: {len(files)}")
        print(f"    Size: {total_size:,} bytes")


def example_streaming_ingestion():
    """Example: streaming ingestion from HuggingFace (if available)."""
    print("\n" + "=" * 70)
    print("Example 5: Streaming Ingestion from HuggingFace")
    print("=" * 70)

    # This would connect to HuggingFace - stub for now
    print("Note: This example requires a real HuggingFace dataset.")
    print("Run `uv run python examples/parquet_ingestion.py --with-hf <dataset_name>`")
    print("to test with a real dataset.")

    # Example code structure:
    """
    loader = HuggingFaceLoader(username="nhagar", suffix="_urls")
    processor = IngestionProcessor()
    writer = ParquetWriter(base_path=Path("./data"))

    for batch_df in loader.load("dataset_name", streaming=True):
        normalized_df = processor.process_batch(batch_df, "dataset_name")
        result = writer.write_batch(normalized_df)
        print(f"Wrote batch: {result['rows_written']} rows")
    """


def main():
    """Run all examples."""
    import argparse

    parser = argparse.ArgumentParser(description="Parquet ingestion examples")
    parser.add_argument(
        "--with-hf",
        metavar="DATASET",
        help="Test with real HuggingFace dataset",
    )
    args = parser.parse_args()

    if args.with_hf:
        # Real HuggingFace ingestion
        print(f"Loading HuggingFace dataset: {args.with_hf}")

        loader = HuggingFaceLoader(username="nhagar", suffix="_urls")
        processor = IngestionProcessor()
        writer = ParquetWriter(base_path=Path("./data"))

        try:
            batch_count = 0
            total_rows = 0

            for batch_df in loader.load(args.with_hf, streaming=True):
                normalized_df = processor.process_batch(batch_df, args.with_hf)
                result = writer.write_batch(normalized_df)

                batch_count += 1
                total_rows += result["total_rows_processed"]

                print(
                    f"Batch {batch_count}: processed {result['total_rows_processed']} rows | "
                    f"buffered: {result['rows_buffered']}, flushed: {result['rows_flushed']} "
                    f"({result['files_written']} files)"
                )

            # Flush any remaining buffered data
            writer.flush()
            writer_stats = writer.get_stats()

            print(f"\nTotal processed: {total_rows} rows in {batch_count} batches")
            print(f"Rows written to disk: {writer_stats['rows_written']}")
            print(f"Storage stats: {writer.get_storage_stats()}")

        except Exception as e:
            print(f"Error loading dataset: {e}")
            print("Make sure the dataset exists and you have access.")

    else:
        # Run all examples
        example_basic_write()
        example_multiple_datasets()
        example_read_back()
        example_compression_stats()
        example_streaming_ingestion()

        print("\n" + "=" * 70)
        print("All examples completed!")
        print("=" * 70)
        print("\nData written to: ./data/urls/")
        print("Explore the directory structure to see partitioning.")


if __name__ == "__main__":
    main()
