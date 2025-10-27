"""
Index builder orchestrator.

Coordinates building all index components:
1. Domain dictionary
2. MPHF
3. Membership index (Roaring bitmaps)
4. File registry
5. Postings index
6. Manifest
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

from .domain_dict import DomainDictionary
from .file_registry import FileRegistry
from .manifest import Manifest
from .membership import MembershipIndex
from .mphf import SimpleMPHF
from .postings import PostingsIndex

logger = logging.getLogger(__name__)


class IndexBuilder:
    """
    Orchestrate building all index components.
    """

    def __init__(
        self,
        base_path: Path,
        num_postings_shards: int = 1024,
        compression_level: int = 6,
    ):
        """
        Initialize index builder.

        Args:
            base_path: Base path for storage
            num_postings_shards: Number of shards for postings index
            compression_level: Zstd compression level
        """
        self.base_path = Path(base_path)
        self.num_postings_shards = num_postings_shards
        self.compression_level = compression_level

        # Initialize components
        self.domain_dict = DomainDictionary(base_path)
        self.mphf = SimpleMPHF()
        self.membership = MembershipIndex(base_path)
        self.file_registry = FileRegistry(base_path)
        self.postings = PostingsIndex(base_path, num_postings_shards)
        self.manifest = Manifest(base_path)

    def build_all(
        self, version: str | None = None, dataset_ids: list[int] | None = None
    ) -> str:
        """
        Build all index components for a new version.

        Args:
            version: Version identifier (defaults to current timestamp)
            dataset_ids: Optional list of dataset IDs to process (None = all)

        Returns:
            Version identifier of the built index
        """
        # Generate version if not provided
        if version is None:
            version = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(f"Building indexes for version {version}")

        # Step 1: Build domain dictionary
        logger.info("Step 1/6: Building domain dictionary...")
        self.domain_dict.build(
            version=version,
            dataset_ids=dataset_ids,
            compression_level=self.compression_level,
        )

        # Step 2: Build MPHF
        logger.info("Step 2/6: Building MPHF...")
        domains = self.domain_dict.read_domain_dict(version)
        self.mphf.build(domains)
        mphf_path = self.base_path / "index" / version / "domains.mphf"
        self.mphf.save(mphf_path, compression_level=self.compression_level)

        # Step 3: Build file registry
        logger.info("Step 3/6: Building file registry...")
        self.file_registry.build(version, self.base_path)

        # Step 4: Build membership index
        logger.info("Step 4/6: Building membership index...")
        domain_lookup = {domain: idx for idx, domain in enumerate(domains)}
        membership_path = self.base_path / "index" / version / "domain_to_datasets.roar"
        self.membership.extract_memberships(domain_lookup)
        self.membership.save(membership_path)

        # Step 5: Build postings index
        logger.info("Step 5/6: Building postings index...")
        file_lookup = {
            info["parquet_rel_path"]: info["file_id"]
            for info in self.file_registry.files
        }
        self.postings.extract_postings(domain_lookup, file_lookup)
        self.postings.save(version, compression_level=self.compression_level)

        # Step 6: Update manifest
        logger.info("Step 6/6: Publishing to manifest...")
        self.manifest.load()
        self.manifest.publish_version(version)

        logger.info(f"Successfully built indexes for version {version}")

        return version

    def build_incremental(self, dataset_ids: list[int] | None = None) -> str:
        """
        Build indexes incrementally by merging with previous version.

        This method:
        1. Determines which Parquet files are new (not in previous file registry)
        2. Loads previous indexes
        3. Processes only new files
        4. Merges with previous data
        5. Writes new version atomically

        Args:
            dataset_ids: Optional list of dataset IDs to process (if None, auto-detect new files)

        Returns:
            Version identifier of the built index
        """
        version = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"Building incremental indexes for version {version}")

        # Load manifest to get previous version
        self.manifest.load()
        prev_version_obj = self.manifest.get_current_version()
        prev_version = prev_version_obj.version if prev_version_obj else None

        if prev_version is None:
            logger.info("No previous version found, building from scratch")
            return self.build_all(version=version, dataset_ids=dataset_ids)

        logger.info(f"Previous version: {prev_version}")

        # Step 1: Determine new files BEFORE building registry
        logger.info("Step 1/6: Determining new files...")
        prev_registry_path = self.base_path / prev_version_obj.files_tsv
        new_files = self.file_registry.get_new_files_since_version(prev_registry_path)
        logger.info(f"Found {len(new_files)} new files to process")

        if not new_files:
            logger.info("No new files to process, skipping index build")
            # No need to build anything if there are no new files
            return prev_version

        # Step 2: Build file registry incrementally
        logger.info("Step 2/6: Building file registry incrementally...")
        self.file_registry.build_incremental(
            version=version,
            base_path=self.base_path,
            prev_registry_path=prev_registry_path,
        )

        # Step 3: Build domain dictionary incrementally
        logger.info("Step 3/6: Building domain dictionary incrementally...")
        self.domain_dict.build_incremental(
            version=version,
            prev_version=prev_version,
            new_files=new_files,
            compression_level=self.compression_level,
        )

        # Step 4: Build MPHF (full rebuild for now)
        logger.info("Step 4/6: Building MPHF...")
        domains = self.domain_dict.read_domain_dict(version)
        self.mphf.build(domains)
        mphf_path = self.base_path / "index" / version / "domains.mphf"
        self.mphf.save(mphf_path, compression_level=self.compression_level)

        # Create domain lookup for downstream indexes
        domain_lookup = {domain: idx for idx, domain in enumerate(domains)}

        # Step 5: Build membership index incrementally
        logger.info("Step 5/6: Building membership index incrementally...")
        prev_membership_path = self.base_path / prev_version_obj.d2d_roar
        prev_domains = self.domain_dict.read_domain_dict(prev_version)
        self.membership.build_incremental(
            domain_lookup=domain_lookup,
            version=version,
            base_path=self.base_path,
            prev_membership_path=prev_membership_path,
            new_files=new_files,
            num_old_domains=len(prev_domains),
        )

        # Step 6: Build postings index incrementally
        logger.info("Step 6/6: Building postings index incrementally...")
        file_lookup = {
            info["parquet_rel_path"]: info["file_id"]
            for info in self.file_registry.files
        }
        self.postings.build_incremental(
            domain_lookup=domain_lookup,
            file_registry=file_lookup,
            version=version,
            prev_version=prev_version,
            new_files=new_files,
            compression_level=self.compression_level,
        )

        # Step 7: Update manifest
        logger.info("Step 7/7: Publishing to manifest...")
        self.manifest.publish_version(version)

        logger.info(
            f"Successfully built incremental indexes for version {version} "
            f"({len(new_files)} new files processed)"
        )

        return version

    def get_stats(self, version: str) -> dict[str, int]:
        """
        Get statistics for a specific index version.

        Args:
            version: Version identifier

        Returns:
            Dictionary of statistics
        """
        logger.info(f"Getting statistics for version {version}")

        stats = {}

        # Domain count
        try:
            domains = self.domain_dict.read_domain_dict(version)
            stats["num_domains"] = len(domains)
        except Exception as e:
            logger.error(f"Error reading domain dict: {e}")
            stats["num_domains"] = 0

        # File count
        try:
            registry_path = self.base_path / "index" / version / "files.tsv.zst"
            self.file_registry.load(registry_path)
            stats["num_files"] = len(self.file_registry.files)
        except Exception as e:
            logger.error(f"Error reading file registry: {e}")
            stats["num_files"] = 0

        # Membership count
        try:
            membership_path = (
                self.base_path / "index" / version / "domain_to_datasets.roar"
            )
            self.membership.load(membership_path, stats.get("num_domains", 0))
            stats["num_domain_dataset_pairs"] = sum(
                len(bm) for bm in self.membership.domain_bitmaps.values()
            )
        except Exception as e:
            logger.error(f"Error reading membership index: {e}")
            stats["num_domain_dataset_pairs"] = 0

        # Postings count
        try:
            postings_dir = self.base_path / "index" / version / "postings"
            if postings_dir.exists():
                shard_dirs = list(postings_dir.iterdir())
                stats["num_postings_shards"] = len(shard_dirs)
            else:
                stats["num_postings_shards"] = 0
        except Exception as e:
            logger.error(f"Error reading postings: {e}")
            stats["num_postings_shards"] = 0

        logger.info(f"Statistics for version {version}: {stats}")

        return stats
