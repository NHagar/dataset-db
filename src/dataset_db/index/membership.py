"""
Domain → Datasets membership index using Roaring bitmaps.

Builds a compact index mapping each domain to the set of datasets containing it.
Format follows spec.md §2.2B:

[magic=DTDR][ver=1][N_domains:uint64][index_offset:uint64]
[bitmaps... concatenated]
[index: N_domains entries of {bitmap_start:uint64, bitmap_len:uint32}]
"""

import logging
import struct
from pathlib import Path

import polars as pl
from pyroaring import BitMap

from ..storage.layout import StorageLayout

logger = logging.getLogger(__name__)


class MembershipIndex:
    """
    Build and query domain → datasets membership index using Roaring bitmaps.
    """

    # File format version
    VERSION = 1
    MAGIC = b"DTDR"

    def __init__(self, base_path: Path):
        """
        Initialize membership index builder.

        Args:
            base_path: Base path for storage
        """
        self.base_path = Path(base_path)
        self.layout = StorageLayout(base_path)
        self.domain_bitmaps: dict[int, BitMap] = {}  # domain_id → BitMap of dataset_ids

    def extract_memberships(self, domain_lookup: dict[str, int]) -> None:
        """
        Extract domain → datasets memberships from Parquet files.

        Args:
            domain_lookup: Map from domain string to domain_id
        """
        logger.info("Extracting domain → datasets memberships...")

        # Get all parquet files
        urls_dir = self.base_path / "urls"
        if not urls_dir.exists():
            logger.warning(f"URLs directory does not exist: {urls_dir}")
            return

        parquet_files = list(urls_dir.rglob("*.parquet"))
        if not parquet_files:
            logger.warning("No Parquet files found")
            return

        logger.info(f"Found {len(parquet_files)} Parquet files to scan")

        # Extract memberships
        for i, parquet_file in enumerate(parquet_files, 1):
            if i % 100 == 0:
                logger.info(
                    f"Processed {i}/{len(parquet_files)} files, "
                    f"{len(self.domain_bitmaps)} domains indexed"
                )

            try:
                # Parse dataset_id from path (dataset_id=N/domain_prefix=XX/part-*.parquet)
                parts = parquet_file.parts
                dataset_id = None
                for part in parts:
                    if part.startswith("dataset_id="):
                        dataset_id = int(part.split("=")[1])
                        break

                if dataset_id is None:
                    logger.warning(f"Could not extract dataset_id from {parquet_file}")
                    continue

                # Read domain column
                df = pl.read_parquet(parquet_file, columns=["domain"])
                unique_domains = df["domain"].unique().to_list()

                # Update bitmaps
                for domain in unique_domains:
                    domain_id = domain_lookup.get(domain)
                    if domain_id is None:
                        logger.warning(
                            f"Domain '{domain}' not found in domain lookup - skipping"
                        )
                        continue

                    if domain_id not in self.domain_bitmaps:
                        self.domain_bitmaps[domain_id] = BitMap()

                    self.domain_bitmaps[domain_id].add(dataset_id)

            except Exception as e:
                logger.error(f"Error processing {parquet_file}: {e}")
                continue

        logger.info(
            f"Extracted memberships for {len(self.domain_bitmaps)} unique domains"
        )

    def save(self, output_path: Path) -> None:
        """
        Save membership index to disk.

        File format:
        [magic=DTDR][ver=1][N_domains:uint64][index_offset:uint64]
        [bitmaps... concatenated]
        [index: N_domains entries of {bitmap_start:uint64, bitmap_len:uint32}]

        Args:
            output_path: Path to save membership index
        """
        logger.info(f"Saving membership index to {output_path}...")

        # Sort domains for consistent ordering
        sorted_domain_ids = sorted(self.domain_bitmaps.keys())

        # Build binary data
        data = bytearray()

        # Header (we'll fill in index_offset later)
        data.extend(self.MAGIC)  # Magic
        data.extend(struct.pack("<I", self.VERSION))  # Version
        data.extend(struct.pack("<Q", len(sorted_domain_ids)))  # N_domains
        index_offset_pos = len(data)
        data.extend(struct.pack("<Q", 0))  # Placeholder for index_offset

        # Write bitmaps and build index
        index_entries = []
        for domain_id in sorted_domain_ids:
            bitmap = self.domain_bitmaps[domain_id]

            # Serialize bitmap
            bitmap_bytes = bitmap.serialize()
            bitmap_start = len(data)
            data.extend(bitmap_bytes)

            # Record index entry
            index_entries.append((bitmap_start, len(bitmap_bytes)))

        # Update index_offset in header
        index_offset = len(data)
        struct.pack_into("<Q", data, index_offset_pos, index_offset)

        # Write index
        for bitmap_start, bitmap_len in index_entries:
            data.extend(struct.pack("<Q", bitmap_start))  # Bitmap start
            data.extend(struct.pack("<I", bitmap_len))  # Bitmap length

        # Write to disk
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(bytes(data))

        # Log statistics
        total_dataset_refs = sum(len(bm) for bm in self.domain_bitmaps.values())
        logger.info(
            f"Saved membership index: {len(data):,} bytes, "
            f"{len(sorted_domain_ids)} domains, "
            f"{total_dataset_refs} total dataset references"
        )

    def load(self, input_path: Path, num_domains: int) -> None:
        """
        Load membership index from disk.

        Args:
            input_path: Path to membership index file
            num_domains: Expected number of domains (for validation)
        """
        logger.info(f"Loading membership index from {input_path}...")

        if not input_path.exists():
            raise FileNotFoundError(f"Membership index not found: {input_path}")

        data = input_path.read_bytes()

        # Parse header
        offset = 0

        magic = data[offset : offset + 4]
        offset += 4
        if magic != self.MAGIC:
            raise ValueError(f"Invalid membership index: bad magic {magic}")

        version = struct.unpack("<I", data[offset : offset + 4])[0]
        offset += 4
        if version != self.VERSION:
            raise ValueError(f"Unsupported membership index version: {version}")

        n_domains = struct.unpack("<Q", data[offset : offset + 8])[0]
        offset += 8

        index_offset = struct.unpack("<Q", data[offset : offset + 8])[0]
        offset += 8

        if n_domains != num_domains:
            logger.warning(
                f"Domain count mismatch: expected {num_domains}, got {n_domains}"
            )

        # Read index
        index_entries = []
        offset = index_offset
        for _ in range(n_domains):
            bitmap_start = struct.unpack("<Q", data[offset : offset + 8])[0]
            offset += 8

            bitmap_len = struct.unpack("<I", data[offset : offset + 4])[0]
            offset += 4

            index_entries.append((bitmap_start, bitmap_len))

        # Load bitmaps
        self.domain_bitmaps = {}
        for domain_id, (bitmap_start, bitmap_len) in enumerate(index_entries):
            bitmap_bytes = data[bitmap_start : bitmap_start + bitmap_len]
            bitmap = BitMap.deserialize(bitmap_bytes)
            self.domain_bitmaps[domain_id] = bitmap

        total_dataset_refs = sum(len(bm) for bm in self.domain_bitmaps.values())
        logger.info(
            f"Loaded membership index: {len(self.domain_bitmaps)} domains, "
            f"{total_dataset_refs} total dataset references"
        )

    def get_datasets(self, domain_id: int) -> list[int]:
        """
        Get list of dataset IDs containing a domain.

        Args:
            domain_id: Domain ID to look up

        Returns:
            List of dataset IDs, or empty list if domain not found
        """
        bitmap = self.domain_bitmaps.get(domain_id)
        if bitmap is None:
            return []
        return list(bitmap)

    def get_dataset_count(self, domain_id: int) -> int:
        """
        Get count of datasets containing a domain.

        Args:
            domain_id: Domain ID to look up

        Returns:
            Count of datasets, or 0 if domain not found
        """
        bitmap = self.domain_bitmaps.get(domain_id)
        if bitmap is None:
            return 0
        return len(bitmap)

    def build(
        self, domain_lookup: dict[str, int], version: str, base_path: Path
    ) -> Path:
        """
        Build complete membership index and save to disk.

        Args:
            domain_lookup: Map from domain string to domain_id
            version: Version identifier
            base_path: Base path for storage

        Returns:
            Path to saved membership index file
        """
        self.extract_memberships(domain_lookup)

        output_path = base_path / "index" / version / "domain_to_datasets.roar"
        self.save(output_path)

        return output_path
