"""
Index loader with lazy loading and caching.

Loads index structures into memory and provides caching for hot domains.
Per spec.md §4.1, maintains warm state (memory-mapped indexes) and LRU caches.
"""

import logging
from functools import lru_cache
from pathlib import Path

import zstandard as zstd

from dataset_db.index.file_registry import FileRegistry
from dataset_db.index.manifest import IndexVersion, Manifest
from dataset_db.index.membership import MembershipIndex
from dataset_db.index.mphf import SimpleMPHF
from dataset_db.index.postings import PostingsIndex

logger = logging.getLogger(__name__)


class IndexLoader:
    """
    Singleton index loader.

    Loads and caches all index structures needed for query serving.
    Implements lazy loading and LRU caching per spec.md §4.1.
    """

    def __init__(self, base_path: Path | str):
        """
        Initialize the loader.

        Args:
            base_path: Base path containing index/ directory
        """
        self.base_path = Path(base_path)
        self.manifest = Manifest(self.base_path)

        # Loaded structures (lazy)
        self._current_version: IndexVersion | None = None
        self._domains: list[str] | None = None  # Domain strings (index = domain_id)
        self._mphf: SimpleMPHF | None = None
        self._membership: MembershipIndex | None = None
        self._file_registry: FileRegistry | None = None
        self._postings: PostingsIndex | None = None

        logger.info(f"IndexLoader initialized with base_path={base_path}")

    def load(self) -> None:
        """
        Load all indexes from the current manifest version.

        This should be called once at server startup.
        """
        logger.info("Loading indexes...")

        # Load manifest
        self.manifest.load()
        self._current_version = self.manifest.get_current_version()
        logger.info(f"Current version: {self._current_version.version}")

        # Load domain dictionary
        dict_path = self.base_path / self._current_version.domains_txt
        self._domains = self._load_domains(dict_path)
        logger.info(f"Loaded {len(self._domains)} domains")

        # Load MPHF
        self._mphf = SimpleMPHF()
        mphf_path = self.base_path / self._current_version.domains_mphf
        self._mphf.load(mphf_path)
        logger.info(f"Loaded MPHF from {mphf_path}")

        # Load membership index
        self._membership = MembershipIndex(self.base_path)
        membership_path = self.base_path / self._current_version.d2d_roar
        self._membership.load(membership_path, len(self._domains))
        logger.info("Loaded membership index")

        # Load file registry
        self._file_registry = FileRegistry(self.base_path)
        registry_path = self.base_path / self._current_version.files_tsv
        self._file_registry.load(registry_path)
        logger.info("Loaded file registry")

        # Load postings index (will lazy-load shards)
        #
        # Note: PostingsIndex expects the *root* data directory so it can
        # construct the canonical layout (index/{version}/postings/...).
        # Passing the version-specific path would result in duplicated
        # segments (index/{version}/index/{version}/...), so we keep the base
        # directory here and supply the version at lookup time.
        self._postings = PostingsIndex(base_path=self.base_path, num_shards=1024)
        logger.info("Postings index ready (shards will be lazy-loaded)")

        logger.info("All indexes loaded successfully")

    def _load_domains(self, dict_path: Path) -> list[str]:
        """
        Load domain dictionary from compressed file.

        Args:
            dict_path: Path to domains.txt.zst

        Returns:
            List of domain strings (sorted)
        """
        if not dict_path.exists():
            raise FileNotFoundError(f"Domain dictionary not found: {dict_path}")

        # Decompress
        decompressor = zstd.ZstdDecompressor()
        compressed_data = dict_path.read_bytes()
        decompressed_data = decompressor.decompress(compressed_data)

        # Parse
        domains_text = decompressed_data.decode("utf-8")
        domains = [line for line in domains_text.split("\n") if line]

        return domains

    @property
    def domains(self) -> list[str]:
        """Get domain list (must be loaded first)."""
        if self._domains is None:
            raise RuntimeError("Indexes not loaded. Call load() first.")
        return self._domains

    @property
    def mphf(self) -> SimpleMPHF:
        """Get MPHF (must be loaded first)."""
        if self._mphf is None:
            raise RuntimeError("Indexes not loaded. Call load() first.")
        return self._mphf

    @property
    def membership(self) -> MembershipIndex:
        """Get membership index (must be loaded first)."""
        if self._membership is None:
            raise RuntimeError("Indexes not loaded. Call load() first.")
        return self._membership

    @property
    def file_registry(self) -> FileRegistry:
        """Get file registry (must be loaded first)."""
        if self._file_registry is None:
            raise RuntimeError("Indexes not loaded. Call load() first.")
        return self._file_registry

    @property
    def postings(self) -> PostingsIndex:
        """Get postings index (must be loaded first)."""
        if self._postings is None:
            raise RuntimeError("Indexes not loaded. Call load() first.")
        return self._postings

    @lru_cache(maxsize=1000)
    def lookup_domain_id(self, domain: str) -> int | None:
        """
        Lookup domain ID with LRU caching.

        Args:
            domain: Domain string to lookup

        Returns:
            Domain ID or None if not found
        """
        return self.mphf.lookup(domain)

    @lru_cache(maxsize=1000)
    def get_domain_string(self, domain_id: int) -> str | None:
        """
        Get domain string from ID with LRU caching.

        Args:
            domain_id: Domain ID

        Returns:
            Domain string or None if not found
        """
        try:
            return self.domains[domain_id]
        except IndexError:
            return None

    @lru_cache(maxsize=1000)
    def get_datasets_for_domain(self, domain_id: int) -> list[int]:
        """
        Get list of dataset IDs containing this domain, with LRU caching.

        Args:
            domain_id: Domain ID

        Returns:
            List of dataset IDs
        """
        return self.membership.get_datasets(domain_id)


# Global singleton instance
_loader: IndexLoader | None = None


def get_loader() -> IndexLoader:
    """
    Get the global IndexLoader instance.

    Returns:
        IndexLoader instance

    Raises:
        RuntimeError: If loader not initialized
    """
    global _loader
    if _loader is None:
        raise RuntimeError("IndexLoader not initialized. Call init_loader() first.")
    return _loader


def init_loader(base_path: Path | str) -> IndexLoader:
    """
    Initialize the global IndexLoader instance.

    Args:
        base_path: Base path containing index/ directory

    Returns:
        Initialized IndexLoader instance
    """
    global _loader
    _loader = IndexLoader(base_path)
    _loader.load()
    return _loader
