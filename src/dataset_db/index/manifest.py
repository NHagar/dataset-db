"""
Manifest system for atomic versioning of indexes.

Implements spec.md §3.3 format for tracking index versions and enabling
atomic updates.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class IndexVersion:
    """
    Represents a single version of the index.
    """

    def __init__(
        self,
        version: str,
        domains_txt: str,
        domains_mphf: str,
        d2d_roar: str,
        postings_base: str,
        files_tsv: str,
        parquet_root: str,
        created_at: str | None = None,
    ):
        """
        Initialize index version.

        Args:
            version: Version identifier (e.g., "2025-10-24T12:00:00Z")
            domains_txt: Path to domains.txt.zst
            domains_mphf: Path to domains.mphf
            d2d_roar: Path to domain_to_datasets.roar
            postings_base: Base path pattern for postings (e.g., "index/2025-10-24/postings/{shard}/postings.{idx,dat}.zst")
            files_tsv: Path to files.tsv.zst
            parquet_root: Root path for Parquet files (e.g., "urls/")
            created_at: ISO timestamp of creation (defaults to now)
        """
        self.version = version
        self.domains_txt = domains_txt
        self.domains_mphf = domains_mphf
        self.d2d_roar = d2d_roar
        self.postings_base = postings_base
        self.files_tsv = files_tsv
        self.parquet_root = parquet_root
        self.created_at = created_at or datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary for JSON serialization."""
        return {
            "version": self.version,
            "domains_txt": self.domains_txt,
            "domains_mphf": self.domains_mphf,
            "d2d_roar": self.d2d_roar,
            "postings_base": self.postings_base,
            "files_tsv": self.files_tsv,
            "parquet_root": self.parquet_root,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "IndexVersion":
        """Create from dictionary."""
        return cls(
            version=data["version"],
            domains_txt=data["domains_txt"],
            domains_mphf=data["domains_mphf"],
            d2d_roar=data["d2d_roar"],
            postings_base=data["postings_base"],
            files_tsv=data["files_tsv"],
            parquet_root=data["parquet_root"],
            created_at=data.get("created_at"),
        )


class Manifest:
    """
    Manage index manifest for atomic versioning.
    """

    def __init__(self, base_path: Path):
        """
        Initialize manifest manager.

        Args:
            base_path: Base path for storage
        """
        self.base_path = Path(base_path)
        self.manifest_path = self.base_path / "index" / "manifest.json"
        self.current_version: str | None = None
        self.versions: list[IndexVersion] = []

    def load(self) -> None:
        """Load manifest from disk."""
        if not self.manifest_path.exists():
            logger.info("No manifest found, starting fresh")
            return

        logger.info(f"Loading manifest from {self.manifest_path}")

        with open(self.manifest_path) as f:
            data = json.load(f)

        self.current_version = data.get("current_version")
        self.versions = [IndexVersion.from_dict(v) for v in data.get("versions", [])]

        logger.info(
            f"Loaded manifest: current_version={self.current_version}, "
            f"{len(self.versions)} versions"
        )

    def save(self) -> None:
        """Save manifest to disk (atomic write)."""
        logger.info(f"Saving manifest to {self.manifest_path}")

        data = {
            "current_version": self.current_version,
            "versions": [v.to_dict() for v in self.versions],
        }

        # Write to temporary file first
        temp_path = self.manifest_path.with_suffix(".tmp")
        temp_path.parent.mkdir(parents=True, exist_ok=True)

        with open(temp_path, "w") as f:
            json.dump(data, f, indent=2)

        # Atomic rename
        temp_path.rename(self.manifest_path)

        logger.info(f"Saved manifest: {len(self.versions)} versions")

    def add_version(self, version: IndexVersion) -> None:
        """
        Add a new version to the manifest.

        Args:
            version: Index version to add
        """
        # Check if version already exists
        existing = self.get_version(version.version)
        if existing:
            logger.warning(f"Version {version.version} already exists, replacing")
            self.versions = [v for v in self.versions if v.version != version.version]

        self.versions.append(version)
        logger.info(f"Added version {version.version} to manifest")

    def set_current_version(self, version: str) -> None:
        """
        Set the current version (atomic flip).

        Args:
            version: Version identifier to set as current
        """
        # Verify version exists
        if not self.get_version(version):
            raise ValueError(f"Version {version} not found in manifest")

        old_version = self.current_version
        self.current_version = version

        logger.info(f"Set current version: {old_version} → {version}")

    def get_version(self, version: str) -> IndexVersion | None:
        """
        Get a specific version.

        Args:
            version: Version identifier

        Returns:
            IndexVersion if found, None otherwise
        """
        for v in self.versions:
            if v.version == version:
                return v
        return None

    def get_current_version(self) -> IndexVersion | None:
        """
        Get the current version.

        Returns:
            Current IndexVersion if set, None otherwise
        """
        if self.current_version is None:
            return None
        return self.get_version(self.current_version)

    def list_versions(self) -> list[str]:
        """
        List all version identifiers.

        Returns:
            List of version identifiers, sorted by creation time
        """
        return [v.version for v in sorted(self.versions, key=lambda x: x.created_at)]

    def create_version_from_build(
        self, version: str, num_shards: int = 1024
    ) -> IndexVersion:
        """
        Create an IndexVersion object for a newly built index.

        Args:
            version: Version identifier
            num_shards: Number of postings shards

        Returns:
            IndexVersion object
        """
        return IndexVersion(
            version=version,
            domains_txt=f"index/{version}/domains.txt.zst",
            domains_mphf=f"index/{version}/domains.mphf",
            d2d_roar=f"index/{version}/domain_to_datasets.roar",
            postings_base=f"index/{version}/postings/{{shard:04d}}/postings.{{idx,dat}}.zst",
            files_tsv=f"index/{version}/files.tsv.zst",
            parquet_root="urls/",
        )

    def publish_version(self, version: str) -> None:
        """
        Publish a version (add to manifest and set as current).

        Args:
            version: Version identifier to publish
        """
        logger.info(f"Publishing version {version}")

        # Create version object
        index_version = self.create_version_from_build(version)

        # Add to manifest
        self.add_version(index_version)

        # Set as current
        self.set_current_version(version)

        # Save atomically
        self.save()

        logger.info(f"Published version {version}")

    def cleanup_old_versions(self, keep_last_n: int = 5) -> list[str]:
        """
        Remove old versions, keeping only the last N.

        Args:
            keep_last_n: Number of versions to keep

        Returns:
            List of removed version identifiers
        """
        if len(self.versions) <= keep_last_n:
            logger.info(f"Only {len(self.versions)} versions, nothing to clean up")
            return []

        # Sort by creation time
        sorted_versions = sorted(
            self.versions, key=lambda x: x.created_at, reverse=True
        )

        # Keep the last N
        to_keep = sorted_versions[:keep_last_n]
        to_remove = sorted_versions[keep_last_n:]

        # Remove from manifest
        removed_ids = [v.version for v in to_remove]
        self.versions = to_keep

        logger.info(f"Removed {len(removed_ids)} old versions: {removed_ids}")

        # Note: This only removes from manifest, not from disk
        # Actual file cleanup should be done separately to avoid accidents

        return removed_ids
