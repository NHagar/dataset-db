"""
Persistent dataset registry for assigning stable dataset IDs.

Ensures dataset IDs are consistent across ingestion runs by storing the
mapping on disk under the configured storage base path (./data by default).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict

from dataset_db.config import get_config

logger = logging.getLogger(__name__)


class DatasetRegistry:
    """Manage dataset name → ID assignments with durable storage."""

    def __init__(
        self,
        base_path: Path | None = None,
        registry_path: Path | None = None,
    ):
        """
        Create a registry stored alongside the data directory.

        Args:
            base_path: Base data directory (defaults to config.storage.base_path)
            registry_path: Optional explicit registry file override
        """
        config = get_config()
        self.base_path = Path(base_path or config.storage.base_path)

        if registry_path is not None:
            self.registry_path = Path(registry_path)
        else:
            self.registry_path = self.base_path / "registry" / "dataset_registry.json"

        self.registry_path.parent.mkdir(parents=True, exist_ok=True)

        self._datasets: Dict[str, int] = {}
        self._next_dataset_id = 0

        self._load()

    def _load(self) -> None:
        """Load registry contents from disk if present."""
        if not self.registry_path.exists():
            return

        try:
            data = json.loads(self.registry_path.read_text())
        except json.JSONDecodeError as exc:
            logger.warning(
                "Failed to parse dataset registry %s: %s. Recreating from scratch.",
                self.registry_path,
                exc,
            )
            return

        datasets = data.get("datasets", {})
        self._datasets = {str(name): int(ds_id) for name, ds_id in datasets.items()}

        next_from_file = int(data.get("next_dataset_id", len(self._datasets)))
        max_existing = max(self._datasets.values(), default=-1) + 1
        self._next_dataset_id = max(next_from_file, max_existing)

    def _save(self) -> None:
        """Persist registry contents atomically."""
        payload = {
            "next_dataset_id": self._next_dataset_id,
            "datasets": self._datasets,
        }
        tmp_path = self.registry_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
        tmp_path.replace(self.registry_path)

    def register_dataset(self, dataset_name: str) -> int:
        """
        Register a dataset name and get its ID (existing IDs are reused).

        Args:
            dataset_name: Dataset identifier string

        Returns:
            Stable dataset ID (UInt32 range)
        """
        if not dataset_name:
            raise ValueError("dataset_name must be a non-empty string")

        if dataset_name in self._datasets:
            return self._datasets[dataset_name]

        dataset_id = self._next_dataset_id
        if dataset_id >= 2**32:
            raise ValueError("Dataset ID overflow (max UInt32)")

        self._datasets[dataset_name] = dataset_id
        self._next_dataset_id += 1
        self._save()
        logger.debug("Registered dataset '%s' with id %s", dataset_name, dataset_id)
        return dataset_id

    def get_dataset_id(self, dataset_name: str) -> int:
        """Look up an existing dataset ID."""
        return self._datasets[dataset_name]

    def to_dict(self) -> Dict[str, int]:
        """Return a copy of the dataset mapping."""
        return dict(self._datasets)

    def reset(self) -> None:
        """Clear the registry contents (primarily for testing)."""
        self._datasets.clear()
        self._next_dataset_id = 0
        if self.registry_path.exists():
            self.registry_path.unlink()
