#!/usr/bin/env python3
"""
Example: Incremental Index Building

Demonstrates how to use incremental index building to efficiently update
indexes when new datasets are added, without rebuilding from scratch.

This example:
1. Ingests an initial dataset and builds indexes
2. Adds a second dataset
3. Builds indexes incrementally (only processing new data)
4. Queries the combined indexes
"""

import logging
from pathlib import Path

import polars as pl

from dataset_db.index import IndexBuilder
from dataset_db.ingestion import IngestionProcessor
from dataset_db.storage import ParquetWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    # Setup
    base_path = Path("./data")
    processor = IngestionProcessor()
    writer = ParquetWriter(base_path=base_path)

    # ====================
    # Phase 1: Initial Ingestion
    # ====================
    logger.info("=" * 80)
    logger.info("Phase 1: Initial Ingestion")
    logger.info("=" * 80)

    # Create first dataset (e-commerce URLs)
    dataset1_urls = [
        "https://shop.example.com/products/laptop",
        "https://shop.example.com/products/phone",
        "https://shop.example.com/cart",
        "https://blog.example.com/article/tech-review",
        "https://support.example.com/faq",
    ]

    df1 = pl.DataFrame({"url": dataset1_urls})

    logger.info(f"Ingesting dataset 'ecommerce' with {len(df1)} URLs...")
    normalized1 = processor.process_batch(df1, "ecommerce")
    writer.write_batch(normalized1)
    writer.flush()

    logger.info(f"Ingested {len(normalized1)} normalized URLs")

    # ====================
    # Phase 2: Initial Index Build
    # ====================
    logger.info("\n" + "=" * 80)
    logger.info("Phase 2: Initial Index Build")
    logger.info("=" * 80)

    builder = IndexBuilder(base_path)
    version1 = builder.build_all()

    logger.info(f"Built initial indexes: version {version1}")

    # Get stats
    stats1 = builder.get_stats(version1)
    logger.info(f"Stats v1: {stats1}")

    # ====================
    # Phase 3: Add New Dataset
    # ====================
    logger.info("\n" + "=" * 80)
    logger.info("Phase 3: Add New Dataset")
    logger.info("=" * 80)

    # Simulate adding a new dataset after some time
    # In production, this might be a separate job or triggered by new data arrival
    dataset2_urls = [
        "https://shop.example.com/products/tablet",  # Same domain as dataset1
        "https://news.example.com/breaking",  # New subdomain
        "https://community.forum.com/thread/123",  # New domain
        "https://api.service.io/v1/users",  # Another new domain
    ]

    df2 = pl.DataFrame({"url": dataset2_urls})

    logger.info(f"Ingesting dataset 'news-and-forums' with {len(df2)} URLs...")
    normalized2 = processor.process_batch(df2, "news-and-forums")
    writer.write_batch(normalized2)
    writer.flush()

    logger.info(f"Ingested {len(normalized2)} normalized URLs")

    # ====================
    # Phase 4: Incremental Index Build
    # ====================
    logger.info("\n" + "=" * 80)
    logger.info("Phase 4: Incremental Index Build")
    logger.info("=" * 80)

    # Build indexes incrementally (only processes new files)
    builder2 = IndexBuilder(base_path)
    version2 = builder2.build_incremental()

    logger.info(f"Built incremental indexes: version {version2}")

    # Get stats
    stats2 = builder2.get_stats(version2)
    logger.info(f"Stats v2: {stats2}")

    # Show improvement
    if version2 != version1:
        domains_added = stats2["num_domains"] - stats1["num_domains"]
        files_added = stats2["num_files"] - stats1["num_files"]
        logger.info(
            f"\nIncremental build added: {domains_added} domains, {files_added} files"
        )
    else:
        logger.info("No new files detected (builds completed in same second)")

    # ====================
    # Phase 5: Query Indexes
    # ====================
    logger.info("\n" + "=" * 80)
    logger.info("Phase 5: Query Combined Indexes")
    logger.info("=" * 80)

    # Load MPHF and domain dict
    from dataset_db.index import MembershipIndex, SimpleMPHF

    # Use latest version
    latest_version = version2 if version2 != version1 else version1

    mphf = SimpleMPHF()
    mphf.load(base_path / "index" / latest_version / "domains.mphf")

    domain_dict = builder2.domain_dict
    domains = domain_dict.read_domain_dict(latest_version)

    logger.info(f"Loaded {len(domains)} domains from version {latest_version}")

    # Query which datasets contain example.com
    example_domain_id = mphf.lookup("example.com")
    logger.info(f"\nLooking up domain 'example.com' (domain_id={example_domain_id})")

    membership = MembershipIndex(base_path)
    membership_path = base_path / "index" / latest_version / "domain_to_datasets.roar"
    membership.load(membership_path, len(domains))

    datasets = membership.get_datasets(example_domain_id)
    logger.info(f"example.com appears in {len(datasets)} dataset(s): {datasets}")

    # Query which datasets contain forum.com
    forum_domain_id = mphf.lookup("forum.com")
    if forum_domain_id is not None:
        logger.info(f"\nLooking up domain 'forum.com' (domain_id={forum_domain_id})")
        datasets = membership.get_datasets(forum_domain_id)
        logger.info(f"forum.com appears in {len(datasets)} dataset(s): {datasets}")

    # ====================
    # Phase 6: Simulate Another Incremental Update
    # ====================
    logger.info("\n" + "=" * 80)
    logger.info("Phase 6: Another Incremental Update")
    logger.info("=" * 80)

    # Add a third dataset
    dataset3_urls = [
        "https://docs.example.com/guide/intro",
        "https://cdn.assets.net/images/logo.png",
    ]

    df3 = pl.DataFrame({"url": dataset3_urls})

    logger.info(f"Ingesting dataset 'documentation' with {len(df3)} URLs...")
    normalized3 = processor.process_batch(df3, "documentation")
    writer.write_batch(normalized3)
    writer.flush()

    # Build incrementally again
    builder3 = IndexBuilder(base_path)
    version3 = builder3.build_incremental()

    logger.info(f"Built incremental indexes: version {version3}")

    stats3 = builder3.get_stats(version3)
    logger.info(f"Stats v3: {stats3}")

    # ====================
    # Summary
    # ====================
    logger.info("\n" + "=" * 80)
    logger.info("Summary: Incremental vs Full Rebuild")
    logger.info("=" * 80)

    logger.info(
        """
Incremental updates provide several benefits:
1. **Performance**: Only process new files, not entire dataset
2. **Efficiency**: Merge new data with existing indexes
3. **Versioning**: Each update creates a new version, supporting rollback
4. **Scalability**: Essential for continuously growing datasets

In this example:
- v1: Initial build (5 URLs, 2-3 domains)
- v2: Incremental update (+4 URLs, +2 domains)
- v3: Another incremental update (+2 URLs, +1-2 domains)

Each incremental build only processed the new files added since the previous version.
    """
    )

    logger.info("\nExample complete!")


if __name__ == "__main__":
    main()
