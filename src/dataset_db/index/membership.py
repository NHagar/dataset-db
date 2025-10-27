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

    def extract_memberships_from_files(
        self, parquet_files: list[Path], domain_lookup: dict[str, int]
    ) -> dict[int, set[int]]:
        """
        Extract domain → datasets memberships from specific Parquet files.

        Args:
            parquet_files: List of Parquet files to scan
            domain_lookup: Map from domain string to domain_id

        Returns:
            Dictionary mapping domain_id → set of dataset_ids
        """
        logger.info(
            f"Extracting memberships from {len(parquet_files)} Parquet files..."
        )

        memberships: dict[int, set[int]] = {}

        for i, parquet_file in enumerate(parquet_files, 1):
            if i % 100 == 0:
                logger.info(
                    f"Processed {i}/{len(parquet_files)} files, "
                    f"{len(memberships)} domains indexed"
                )

            try:
                # Parse dataset_id from path
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

                # Update memberships
                for domain in unique_domains:
                    domain_id = domain_lookup.get(domain)
                    if domain_id is None:
                        logger.warning(
                            f"Domain '{domain}' not found in domain lookup - skipping"
                        )
                        continue

                    if domain_id not in memberships:
                        memberships[domain_id] = set()

                    memberships[domain_id].add(dataset_id)

            except Exception as e:
                logger.error(f"Error processing {parquet_file}: {e}")
                continue

        logger.info(
            f"Extracted memberships for {len(memberships)} domains from new files"
        )

        return memberships

    def merge_memberships(
        self,
        old_bitmaps: dict[int, BitMap],
        new_memberships: dict[int, set[int]],
    ) -> dict[int, BitMap]:
        """
        Merge old bitmaps with new memberships.

        Args:
            old_bitmaps: Existing domain_id → BitMap mapping
            new_memberships: New domain_id → set of dataset_ids

        Returns:
            Merged domain_id → BitMap mapping
        """
        logger.info(
            f"Merging {len(old_bitmaps)} old bitmaps with "
            f"{len(new_memberships)} new memberships"
        )

        # Start with copy of old bitmaps
        merged = {}
        for domain_id, bitmap in old_bitmaps.items():
            # Create a copy of the bitmap
            merged[domain_id] = BitMap(bitmap)

        # Merge in new memberships
        num_updated = 0
        num_new = 0

        for domain_id, dataset_ids in new_memberships.items():
            if domain_id in merged:
                # Update existing bitmap
                for dataset_id in dataset_ids:
                    merged[domain_id].add(dataset_id)
                num_updated += 1
            else:
                # Create new bitmap
                merged[domain_id] = BitMap(dataset_ids)
                num_new += 1

        logger.info(
            f"Merged result: {len(merged)} total domains "
            f"({num_updated} updated, {num_new} new)"
        )

        return merged

    def build_incremental(
        self,
        domain_lookup: dict[str, int],
        version: str,
        base_path: Path,
        prev_membership_path: Path | None,
        new_files: list[Path],
        num_old_domains: int = 0,
    ) -> Path:
        """
        Build membership index incrementally by merging with previous version.

        Args:
            domain_lookup: Map from domain string to domain_id (from new MPHF)
            version: New version identifier
            base_path: Base path for storage
            prev_membership_path: Path to previous membership index (optional)
            new_files: List of new Parquet files to process
            num_old_domains: Number of domains in previous version (for validation)

        Returns:
            Path to saved membership index file
        """
        logger.info("Building membership index incrementally...")

        # Load previous bitmaps if available
        old_bitmaps = {}
        if prev_membership_path and prev_membership_path.exists():
            try:
                logger.info(f"Loading previous membership index from {prev_membership_path}")
                self.load(prev_membership_path, num_old_domains)
                old_bitmaps = self.domain_bitmaps.copy()
                logger.info(f"Loaded {len(old_bitmaps)} bitmaps from previous version")
            except Exception as e:
                logger.warning(
                    f"Failed to load previous membership index: {e}, starting from scratch"
                )

        # Extract memberships from new files only
        new_memberships = self.extract_memberships_from_files(new_files, domain_lookup)

        # Merge old + new
        self.domain_bitmaps = self.merge_memberships(old_bitmaps, new_memberships)

        # Save merged index
        output_path = base_path / "index" / version / "domain_to_datasets.roar"
        self.save(output_path)

        logger.info(
            f"Built incremental membership index: "
            f"{len(old_bitmaps)} old + {len(new_memberships)} new = "
            f"{len(self.domain_bitmaps)} total domains"
        )

        return output_path
