"""
Domain dictionary builder and reader.

Extracts unique domains from Parquet files, sorts them, and creates:
- domains.txt.zst: sorted unique domains, newline-delimited, zstd compressed
- Supports forward lookup (string → id) and reverse lookup (id → string)
"""

import logging
from pathlib import Path
from typing import Iterator

import polars as pl
import zstandard as zstd

from ..storage.layout import StorageLayout

logger = logging.getLogger(__name__)


class DomainDictionary:
    """
    Build and manage domain dictionaries.

    The domain dictionary maps domain strings to sequential integer IDs.
    The ID is simply the index in the sorted list of unique domains.
    """

    def __init__(self, base_path: Path):
        """
        Initialize domain dictionary builder.

        Args:
            base_path: Base path for storage (e.g., './data')
        """
        self.base_path = Path(base_path)
        self.layout = StorageLayout(base_path)

    def extract_unique_domains(
        self, dataset_ids: list[int] | None = None
    ) -> list[str]:
        """
        Extract unique domains from all Parquet files.

        Args:
            dataset_ids: Optional list of dataset IDs to process. If None, processes all.

        Returns:
            Sorted list of unique domain strings
        """
        logger.info("Extracting unique domains from Parquet files...")

        # Get all parquet files
        if dataset_ids is None:
            # Scan all dataset directories
            urls_dir = self.base_path / "urls"
            if not urls_dir.exists():
                logger.warning(f"URLs directory does not exist: {urls_dir}")
                return []

            parquet_files = list(urls_dir.rglob("*.parquet"))
        else:
            # Scan specific datasets
            parquet_files = []
            for dataset_id in dataset_ids:
                partitions = self.layout.list_partitions(dataset_id)
                for ds_id, domain_prefix in partitions:
                    files = self.layout.list_parquet_files(ds_id, domain_prefix)
                    parquet_files.extend(files)

        if not parquet_files:
            logger.warning("No Parquet files found")
            return []

        logger.info(f"Found {len(parquet_files)} Parquet files to scan")

        # Extract unique domains using Polars
        unique_domains = set()

        for i, parquet_file in enumerate(parquet_files, 1):
            if i % 100 == 0:
                logger.info(
                    f"Processed {i}/{len(parquet_files)} files, "
                    f"{len(unique_domains)} unique domains so far"
                )

            try:
                # Read only the domain column
                df = pl.read_parquet(parquet_file, columns=["domain"])
                domains = df["domain"].unique().to_list()
                unique_domains.update(domains)
            except Exception as e:
                logger.error(f"Error reading {parquet_file}: {e}")
                continue

        # Sort domains for consistent ordering
        sorted_domains = sorted(unique_domains)
        logger.info(f"Extracted {len(sorted_domains)} unique domains")

        return sorted_domains

    def write_domain_dict(
        self, domains: list[str], version: str, compression_level: int = 6
    ) -> Path:
        """
        Write domain dictionary to compressed file.

        Args:
            domains: Sorted list of unique domains
            version: Version identifier (e.g., "2025-10-24T12:00:00Z")
            compression_level: Zstd compression level (1-22)

        Returns:
            Path to the written file
        """
        # Create index directory for this version
        index_dir = self.base_path / "index" / version
        index_dir.mkdir(parents=True, exist_ok=True)

        output_path = index_dir / "domains.txt.zst"

        logger.info(
            f"Writing {len(domains)} domains to {output_path} "
            f"(compression level {compression_level})"
        )

        # Compress and write
        compressor = zstd.ZstdCompressor(level=compression_level)

        # Join domains with newlines
        domains_text = "\n".join(domains) + "\n"  # Add trailing newline
        domains_bytes = domains_text.encode("utf-8")

        compressed_data = compressor.compress(domains_bytes)

        output_path.write_bytes(compressed_data)

        # Log statistics
        original_size = len(domains_bytes)
        compressed_size = len(compressed_data)
        ratio = original_size / compressed_size if compressed_size > 0 else 0

        logger.info(
            f"Wrote domain dictionary: {original_size:,} bytes → {compressed_size:,} bytes "
            f"(compression ratio: {ratio:.2f}x)"
        )

        return output_path

    def read_domain_dict(self, version: str) -> list[str]:
        """
        Read domain dictionary from compressed file.

        Args:
            version: Version identifier

        Returns:
            List of domain strings (sorted)
        """
        dict_path = self.base_path / "index" / version / "domains.txt.zst"

        if not dict_path.exists():
            raise FileNotFoundError(f"Domain dictionary not found: {dict_path}")

        logger.info(f"Reading domain dictionary from {dict_path}")

        # Decompress
        decompressor = zstd.ZstdDecompressor()
        compressed_data = dict_path.read_bytes()
        decompressed_data = decompressor.decompress(compressed_data)

        # Parse
        domains_text = decompressed_data.decode("utf-8")
        domains = [line for line in domains_text.split("\n") if line]

        logger.info(f"Loaded {len(domains)} domains")

        return domains

    def iter_domains(self, version: str) -> Iterator[tuple[int, str]]:
        """
        Iterate over domains with their IDs.

        Args:
            version: Version identifier

        Yields:
            Tuples of (domain_id, domain_string)
        """
        domains = self.read_domain_dict(version)
        for domain_id, domain in enumerate(domains):
            yield domain_id, domain

    def build(
        self,
        version: str,
        dataset_ids: list[int] | None = None,
        compression_level: int = 6,
    ) -> Path:
        """
        Build complete domain dictionary from Parquet files.

        Args:
            version: Version identifier
            dataset_ids: Optional list of dataset IDs to process
            compression_level: Zstd compression level

        Returns:
            Path to the written domain dictionary file
        """
        domains = self.extract_unique_domains(dataset_ids)
        return self.write_domain_dict(domains, version, compression_level)
