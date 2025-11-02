"""Utilities for tracking ingested URLs and preventing duplicates."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Optional

import xxhash

from dataset_db.config import get_config


class DuplicateTracker:
    """Persistently track which URL IDs have been ingested per dataset."""

    def __init__(self, base_path: Optional[Path] = None) -> None:
        config = get_config()
        self._root = Path(base_path or config.storage.base_path) / "ingestion" / "duplicates"
        self._root.mkdir(parents=True, exist_ok=True)
        self._cache: dict[str, set[int]] = {}

    def _dataset_key(self, dataset_name: str) -> str:
        return xxhash.xxh64(dataset_name.encode("utf-8")).hexdigest()

    def _dataset_path(self, dataset_name: str) -> Path:
        return self._root / f"{self._dataset_key(dataset_name)}.txt"

    def _load_dataset(self, dataset_name: str) -> set[int]:
        if dataset_name in self._cache:
            return self._cache[dataset_name]

        file_path = self._dataset_path(dataset_name)
        seen: set[int] = set()

        if file_path.exists():
            with file_path.open("r", encoding="utf-8") as fp:
                for line in fp:
                    value = line.strip()
                    if not value:
                        continue
                    try:
                        seen.add(int(value))
                    except ValueError:
                        continue

        self._cache[dataset_name] = seen
        return seen

    def is_duplicate(self, dataset_name: str, url_id: int) -> bool:
        """Return True if the URL ID was already ingested for the dataset."""

        seen = self._load_dataset(dataset_name)
        return url_id in seen

    def record_batch(self, dataset_name: str, url_ids: Iterable[int]) -> None:
        """Persist URL IDs ingested for the dataset, skipping ones already present."""

        seen = self._load_dataset(dataset_name)
        new_ids: list[int] = []

        for url_id in url_ids:
            if url_id in seen:
                continue
            seen.add(url_id)
            new_ids.append(url_id)

        if not new_ids:
            return

        file_path = self._dataset_path(dataset_name)
        with file_path.open("a", encoding="utf-8") as fp:
            for url_id in new_ids:
                fp.write(f"{url_id}\n")

    def reset(self, dataset_name: str) -> None:
        """Remove tracking information for a dataset (primarily for testing)."""

        self._cache.pop(dataset_name, None)
        file_path = self._dataset_path(dataset_name)
        if file_path.exists():
            file_path.unlink()
