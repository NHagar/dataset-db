"""
HuggingFace dataset loader.

Loads datasets from HuggingFace Hub following the naming convention:
{username}/{dataset_name}_urls
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
        suffix: str = "_urls",
        batch_size: Optional[int] = None,
    ):
        """
        Initialize HuggingFace loader.

        Args:
            username: HuggingFace username (default: 'nhagar')
            suffix: Dataset name suffix (default: '_urls')
            batch_size: Batch size for streaming (default: from config, 1M rows)
        """
        self.username = username
        self.suffix = suffix
        self.config = get_config()
        self.batch_size = batch_size or self.config.ingestion.batch_size

    def load(
        self,
        dataset_name: str,
        split: str = "train",
    ) -> Iterator[pl.DataFrame]:
        """
        Load a dataset from HuggingFace Hub.

        Args:
            dataset_name: Dataset name (without username prefix or suffix)
            split: Dataset split to load (default: 'train')

        Yields:
            Polars DataFrames with batches of data

        Raises:
            ValueError: If dataset cannot be loaded
        """
        # Construct full dataset identifier
        full_name = f"{self.username}/{dataset_name}{self.suffix}"

        try:
            # Load dataset from HuggingFace
            dataset = load_dataset(
                full_name,
                split=split,
                streaming=True,
            )

            # Yield batches from streaming dataset
            yield from self._stream_batches(
                dataset, dataset_name=dataset_name, batch_size=self.batch_size
            )

        except Exception as e:
            raise ValueError(f"Failed to load dataset '{full_name}': {e}")

    def _get_state_dict_path(self, dataset_name: str) -> Path:
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

    def _load_state_dict(self, dataset_name: str) -> Optional[dict]:
        """
        Load state dict if it exists.

        Args:
            dataset_name: Dataset name

        Returns:
            State dict or None if file doesn't exist
        """
        state_path = self._get_state_dict_path(dataset_name)
        if state_path.exists():
            with open(state_path, "r") as f:
                return json.load(f)
        return None

    def _save_state_dict(self, dataset_name: str, state_dict: dict) -> None:
        """
        Save state dict to disk.

        Args:
            dataset_name: Dataset name
            state_dict: State dict to save
        """
        state_path = self._get_state_dict_path(dataset_name)
        with open(state_path, "w") as f:
            json.dump(state_dict, f)

    def _stream_batches(
        self, dataset, dataset_name: str, batch_size: int
    ) -> Iterator[pl.DataFrame]:
        """
        Stream batches from HuggingFace dataset with resume support.

        Args:
            dataset: HuggingFace streaming dataset
            dataset_name: Dataset name (for state dict persistence)
            batch_size: Number of records per batch

        Yields:
            Polars DataFrames with batches
        """
        # Check for existing state dict and resume if available
        state_dict = self._load_state_dict(dataset_name)
        if state_dict is not None:
            dataset.load_state_dict(state_dict)

        batch = []

        for record in dataset:
            batch.append(record)

            if len(batch) >= batch_size:
                # Convert batch to Polars DataFrame
                df = pl.DataFrame(batch)
                yield df
                batch = []

                # Save state dict after yielding batch
                self._save_state_dict(dataset_name, dataset.state_dict())

        # Yield remaining records
        if batch:
            df = pl.DataFrame(batch)
            yield df

        # Clean up state dict file after successful completion
        state_path = self._get_state_dict_path(dataset_name)
        if state_path.exists():
            state_path.unlink()

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
