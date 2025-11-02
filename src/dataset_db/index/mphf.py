"""
Minimal Perfect Hash Function (MPHF) implementation.

Uses a simple hash-based approach with collision handling as suggested in spec.md §1.2:
- Primary: xxh3_64(domain) → domain_id lookup
- Collision map: Small map for hash collisions (rare)
- Tag: 16-bit tag (xxh3_64 >> 48) for early rejection

For production scale, could be replaced with BBHash or similar MPHF library.
This implementation prioritizes simplicity and correctness.
"""

import logging
import struct
from pathlib import Path
from typing import Optional

import xxhash
import zstandard as zstd

logger = logging.getLogger(__name__)


class SimpleMPHF:
    """
    Simple hash-based domain lookup with collision handling.

    Maps domain strings to their sequential IDs in the domain dictionary.
    """

    # File format version
    VERSION = 1

    def __init__(self):
        """Initialize MPHF."""
        self.domain_to_id: dict[str, int] = {}
        self.hash_to_id: dict[int, int] = {}  # hash64 → domain_id
        self.collision_map: dict[int, list[tuple[int, str, int]]] = (
            {}
        )  # hash64 → [(tag, domain, id), ...]

    def build(self, domains: list[str]) -> None:
        """
        Build MPHF from sorted domain list.

        Args:
            domains: Sorted list of unique domains (from domain dictionary)
        """
        logger.info(f"Building MPHF for {len(domains)} domains...")

        collision_count = 0

        for domain_id, domain in enumerate(domains):
            # Store in main lookup
            self.domain_to_id[domain] = domain_id

            # Compute hash and tag
            hash_val = xxhash.xxh3_64_intdigest(domain.encode("utf-8"))
            tag = (hash_val >> 48) & 0xFFFF  # 16-bit tag from high bits

            # Check for hash collision
            if hash_val in self.hash_to_id:
                # Collision detected - add to collision map
                collision_count += 1

                if hash_val not in self.collision_map:
                    # Move existing entry to collision map
                    existing_id = self.hash_to_id[hash_val]
                    existing_domain = domains[existing_id]
                    existing_hash = xxhash.xxh3_64_intdigest(
                        existing_domain.encode("utf-8")
                    )
                    existing_tag = (existing_hash >> 48) & 0xFFFF

                    self.collision_map[hash_val] = [
                        (existing_tag, existing_domain, existing_id)
                    ]

                # Add current entry to collision map
                self.collision_map[hash_val].append((tag, domain, domain_id))
            else:
                # No collision - direct mapping
                self.hash_to_id[hash_val] = domain_id

        logger.info(
            f"MPHF built: {len(domains)} domains, {collision_count} hash collisions"
        )

        if collision_count > 0:
            logger.warning(
                f"Found {collision_count} hash collisions - this is expected but rare"
            )

    def lookup(self, domain: str) -> Optional[int]:
        """
        Look up domain ID by domain string.

        Args:
            domain: Domain string to look up

        Returns:
            Domain ID if found, None otherwise
        """
        # Fast path: check direct map
        if domain in self.domain_to_id:
            return self.domain_to_id[domain]

        # Compute hash
        hash_val = xxhash.xxh3_64_intdigest(domain.encode("utf-8"))
        tag = (hash_val >> 48) & 0xFFFF

        # Check direct hash mapping
        if hash_val in self.hash_to_id and hash_val not in self.collision_map:
            return self.hash_to_id[hash_val]

        # Check collision map
        if hash_val in self.collision_map:
            for stored_tag, stored_domain, domain_id in self.collision_map[hash_val]:
                if tag == stored_tag and domain == stored_domain:
                    return domain_id

        return None

    def save(self, output_path: Path, compression_level: int = 6) -> None:
        """
        Save MPHF to disk with compression.

        File format:
        - Header: [magic=MPHF][version:u32][num_domains:u64][num_collisions:u32]
        - Hash map: [hash:u64, domain_id:u32] * num_domains
        - Collision map: [hash:u64, num_entries:u16, [(tag:u16, domain_len:u16, domain:bytes, id:u32)] * num_entries] * num_collisions

        Args:
            output_path: Path to save MPHF file
            compression_level: Zstd compression level
        """
        logger.info(f"Saving MPHF to {output_path}...")

        # Build binary data
        data = bytearray()

        # Header
        data.extend(b"MPHF")  # Magic
        data.extend(struct.pack("<I", self.VERSION))  # Version
        data.extend(struct.pack("<Q", len(self.hash_to_id)))  # Num domains
        data.extend(struct.pack("<I", len(self.collision_map)))  # Num collisions

        # Hash map (non-collision entries)
        for hash_val, domain_id in sorted(self.hash_to_id.items()):
            if hash_val not in self.collision_map:
                data.extend(struct.pack("<Q", hash_val))  # Hash
                data.extend(struct.pack("<I", domain_id))  # Domain ID

        # Collision map
        for hash_val, entries in sorted(self.collision_map.items()):
            data.extend(struct.pack("<Q", hash_val))  # Hash
            data.extend(struct.pack("<H", len(entries)))  # Num entries

            for tag, domain, domain_id in entries:
                domain_bytes = domain.encode("utf-8")
                data.extend(struct.pack("<H", tag))  # Tag
                data.extend(struct.pack("<H", len(domain_bytes)))  # Domain length
                data.extend(domain_bytes)  # Domain
                data.extend(struct.pack("<I", domain_id))  # Domain ID

        # Compress
        compressor = zstd.ZstdCompressor(level=compression_level)
        compressed_data = compressor.compress(bytes(data))

        # Write
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(compressed_data)

        # Log statistics
        original_size = len(data)
        compressed_size = len(compressed_data)
        ratio = original_size / compressed_size if compressed_size > 0 else 0

        logger.info(
            f"Saved MPHF: {original_size:,} bytes → {compressed_size:,} bytes "
            f"(compression ratio: {ratio:.2f}x)"
        )

    def load(self, input_path: Path) -> None:
        """
        Load MPHF from disk.

        Args:
            input_path: Path to MPHF file
        """
        logger.info(f"Loading MPHF from {input_path}...")

        if not input_path.exists():
            raise FileNotFoundError(f"MPHF file not found: {input_path}")

        # Decompress
        decompressor = zstd.ZstdDecompressor()
        compressed_data = input_path.read_bytes()
        data = decompressor.decompress(compressed_data)

        # Parse header
        offset = 0

        magic = data[offset : offset + 4]
        offset += 4
        if magic != b"MPHF":
            raise ValueError(f"Invalid MPHF file: bad magic {magic}")

        version = struct.unpack("<I", data[offset : offset + 4])[0]
        offset += 4
        if version != self.VERSION:
            raise ValueError(f"Unsupported MPHF version: {version}")

        num_domains = struct.unpack("<Q", data[offset : offset + 8])[0]
        offset += 8

        num_collisions = struct.unpack("<I", data[offset : offset + 4])[0]
        offset += 4

        # Parse hash map
        self.hash_to_id = {}
        self.domain_to_id = {}
        for _ in range(num_domains - num_collisions):
            hash_val = struct.unpack("<Q", data[offset : offset + 8])[0]
            offset += 8

            domain_id = struct.unpack("<I", data[offset : offset + 4])[0]
            offset += 4

            self.hash_to_id[hash_val] = domain_id

        # Parse collision map
        self.collision_map = {}
        for _ in range(num_collisions):
            hash_val = struct.unpack("<Q", data[offset : offset + 8])[0]
            offset += 8

            num_entries = struct.unpack("<H", data[offset : offset + 2])[0]
            offset += 2

            entries = []
            for _ in range(num_entries):
                tag = struct.unpack("<H", data[offset : offset + 2])[0]
                offset += 2

                domain_len = struct.unpack("<H", data[offset : offset + 2])[0]
                offset += 2

                domain = data[offset : offset + domain_len].decode("utf-8")
                offset += domain_len

                domain_id = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4

                entries.append((tag, domain, domain_id))
                # Also populate domain_to_id for fast lookups
                self.domain_to_id[domain] = domain_id

            self.collision_map[hash_val] = entries
            # Mark hash as having collisions
            self.hash_to_id[hash_val] = entries[0][
                2
            ]  # Store first entry in hash_to_id

        logger.info(
            f"Loaded MPHF: {num_domains} domains, "
            f"{len(self.collision_map)} hash collisions"
        )

    @staticmethod
    def build_from_dict(domains: list[str], version: str, base_path: Path) -> Path:
        """
        Build MPHF from domain dictionary and save to disk.

        Args:
            domains: Sorted list of unique domains
            version: Version identifier
            base_path: Base path for storage

        Returns:
            Path to saved MPHF file
        """
        mphf = SimpleMPHF()
        mphf.build(domains)

        output_path = base_path / "index" / version / "domains.mphf"
        mphf.save(output_path)

        return output_path
