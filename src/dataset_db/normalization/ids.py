"""
ID generation utilities.

Implements the ID schemes from spec.md §1.2:
- dataset_id: UInt32 (from registry)
- domain_id: MPHF index or hash64
- url_id: xxh3_64(raw_url_bytes)
"""

from typing import Dict

import xxhash


class IDGenerator:
    """
    Generate IDs for URLs, domains, and datasets.

    For ingestion phase, we use simple hashing. The MPHF-based domain_id
    will be generated during index building phase.
    """

    def __init__(self):
        """Initialize ID generator."""
        self._dataset_registry: Dict[str, int] = {}
        self._next_dataset_id = 0

    def get_url_id(self, url: str) -> int:
        """
        Generate URL ID using xxh3_64 hash.

        Args:
            url: Raw URL string

        Returns:
            64-bit hash as signed int64 (for Parquet compatibility)
        """
        # xxhash returns unsigned, we'll store as signed int64 in Parquet
        hash_val = xxhash.xxh3_64(url.encode("utf-8")).intdigest()
        # Convert to signed int64 range
        if hash_val >= 2**63:
            hash_val -= 2**64
        return hash_val

    def get_domain_id(self, domain: str) -> int:
        """
        Generate domain ID using xxh3_64 hash.

        Note: During ingestion, we use hash-based IDs. The final MPHF-based
        domain_id will be assigned during index building.

        Args:
            domain: Normalized domain string (eTLD+1)

        Returns:
            64-bit hash as signed int64
        """
        hash_val = xxhash.xxh3_64(domain.encode("utf-8")).intdigest()
        # Convert to signed int64 range
        if hash_val >= 2**63:
            hash_val -= 2**64
        return hash_val

    def get_domain_prefix(self, domain: str, prefix_chars: int = 2) -> str:
        """
        Get domain prefix for partitioning (first N hex chars of domain hash).

        Args:
            domain: Normalized domain string
            prefix_chars: Number of hex characters to use (default: 2)

        Returns:
            Hex prefix string (e.g., 'a7', '3f')
        """
        hash_val = xxhash.xxh3_64(domain.encode("utf-8")).intdigest()
        # Get first N hex chars
        hex_str = f"{hash_val:016x}"
        return hex_str[:prefix_chars]

    def register_dataset(self, dataset_name: str) -> int:
        """
        Register a dataset and get its ID.

        Args:
            dataset_name: Name of the dataset

        Returns:
            Dataset ID (UInt32)
        """
        if dataset_name in self._dataset_registry:
            return self._dataset_registry[dataset_name]

        dataset_id = self._next_dataset_id
        self._dataset_registry[dataset_name] = dataset_id
        self._next_dataset_id += 1

        if self._next_dataset_id >= 2**32:
            raise ValueError("Dataset ID overflow (max 2^32)")

        return dataset_id

    def get_dataset_id(self, dataset_name: str) -> int:
        """
        Get dataset ID for a registered dataset.

        Args:
            dataset_name: Name of the dataset

        Returns:
            Dataset ID

        Raises:
            KeyError: If dataset not registered
        """
        if dataset_name not in self._dataset_registry:
            raise KeyError(f"Dataset '{dataset_name}' not registered")
        return self._dataset_registry[dataset_name]

    def get_all_datasets(self) -> Dict[str, int]:
        """Get all registered datasets."""
        return self._dataset_registry.copy()


# Global ID generator instance
_id_generator: IDGenerator = None


def get_id_generator() -> IDGenerator:
    """Get or create global ID generator instance."""
    global _id_generator
    if _id_generator is None:
        _id_generator = IDGenerator()
    return _id_generator


def reset_id_generator() -> None:
    """Reset global ID generator (for testing)."""
    global _id_generator
    _id_generator = None
