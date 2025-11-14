"""
HuggingFace dataset loader.

Loads datasets from HuggingFace Hub with the format:
{username}/{dataset_name}
"""

import json
from pathlib import Path
from typing import Iterator, Optional

import polars as pl
from datasets import load_dataset

from dataset_db.config import get_config


class HuggingFaceLoader:
    """
    Load datasets from HuggingFace Hub.

    Expected schema:
    - url: string (record URL)
    - domain: string (top-level domain)
    """

    def __init__(
        self,
        username: str = "nhagar",
        batch_size: Optional[int] = None,
    ):
        """
        Initialize HuggingFace loader.

        Args:
            username: HuggingFace username (default: 'nhagar')
            batch_size: Batch size for streaming (default: from config, 1M rows)
        """
        self.username = username
        self.config = get_config()
        self.batch_size = batch_size or self.config.ingestion.batch_size

    def load(
        self,
        dataset_name: str,
        split: str = "train",
        resume: bool = True,
    ) -> Iterator[pl.DataFrame]:
        """
        Load a dataset from HuggingFace Hub.

        Args:
            dataset_name: Full dataset name after username (e.g., 'reddit_urls', 'twitter_urls', or 'custom_dataset')
            split: Dataset split to load (default: 'train')
            resume: Whether to attempt resume from saved state (default: True)

        Yields:
            Polars DataFrames with batches of data

        Raises:
            ValueError: If dataset cannot be loaded
        """
        # Construct full dataset identifier
        full_name = f"{self.username}/{dataset_name}"

        try:
            # Load dataset from HuggingFace
            dataset = load_dataset(
                full_name,
                split=split,
                streaming=True,
            )

            # Resume from saved state if requested
            if resume:
                state_dict = self.load_state_dict(dataset_name)
                if state_dict is not None:
                    dataset.load_state_dict(state_dict)

            # Store dataset for state dict access
            self._current_dataset = dataset
            self._current_dataset_name = dataset_name

            # Yield batches from streaming dataset
            yield from self._stream_batches(dataset, batch_size=self.batch_size)

        except Exception as e:
            raise ValueError(f"Failed to load dataset '{full_name}': {e}")

    def get_state_dict_path(self, dataset_name: str) -> Path:
        """
        Get path to state dict file for a dataset.

        Args:
            dataset_name: Dataset name

        Returns:
            Path to state dict JSON file
        """
        data_dir = self.config.storage.base_path
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir / f"{dataset_name}-state-dict.json"

    def load_state_dict(self, dataset_name: str) -> Optional[dict]:
        """
        Load state dict if it exists.

        Args:
            dataset_name: Dataset name

        Returns:
            State dict or None if file doesn't exist
        """
        state_path = self.get_state_dict_path(dataset_name)
        if state_path.exists():
            with open(state_path, "r") as f:
                return json.load(f)
        return None

    def save_state_dict(self, dataset_name: str) -> None:
        """
        Save current dataset state dict to disk.

        This should be called by the consumer after successfully writing data
        to ensure state is synchronized with persisted data.

        Args:
            dataset_name: Dataset name

        Raises:
            RuntimeError: If no dataset is currently loaded
        """
        if not hasattr(self, "_current_dataset") or self._current_dataset is None:
            raise RuntimeError("No dataset currently loaded")

        state_dict = self._current_dataset.state_dict()
        state_path = self.get_state_dict_path(dataset_name)
        with open(state_path, "w") as f:
            json.dump(state_dict, f)

    def clear_state_dict(self, dataset_name: str) -> None:
        """
        Delete the state dict file for a dataset.

        This should be called after successful completion of ingestion.

        Args:
            dataset_name: Dataset name
        """
        state_path = self.get_state_dict_path(dataset_name)
        if state_path.exists():
            state_path.unlink()

    def _stream_batches(self, dataset, batch_size: int) -> Iterator[pl.DataFrame]:
        """
        Stream batches from HuggingFace dataset.

        Args:
            dataset: HuggingFace streaming dataset
            batch_size: Number of records per batch

        Yields:
            Polars DataFrames with batches
        """
        batch = []

        for record in dataset:
            batch.append(record)

            if len(batch) >= batch_size:
                # Convert batch to Polars DataFrame
                df = pl.DataFrame(batch)
                yield df
                batch = []

        # Yield remaining records
        if batch:
            df = pl.DataFrame(batch)
            yield df

    def validate_schema(self, df: pl.DataFrame) -> bool:
        """
        Validate that DataFrame has expected schema.

        Args:
            df: Polars DataFrame to validate

        Returns:
            True if schema is valid

        Raises:
            ValueError: If schema is invalid
        """
        required_columns = {"url", "domain"}
        actual_columns = set(df.columns)

        if not required_columns.issubset(actual_columns):
            missing = required_columns - actual_columns
            raise ValueError(f"Missing required columns: {missing}")

        # Check types
        if df.schema["url"] != pl.Utf8:
            raise ValueError(f"Column 'url' must be String, got {df.schema['url']}")

        if df.schema["domain"] != pl.Utf8:
            raise ValueError(
                f"Column 'domain' must be String, got {df.schema['domain']}"
            )

        return True
