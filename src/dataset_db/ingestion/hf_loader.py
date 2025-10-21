"""
HuggingFace dataset loader.

Loads datasets from HuggingFace Hub following the naming convention:
{username}/{dataset_name}_urls
"""

from typing import Iterator, Optional

import polars as pl
from datasets import load_dataset


class HuggingFaceLoader:
    """
    Load datasets from HuggingFace Hub.

    Expected schema:
    - url: string (record URL)
    - domain: string (top-level domain)
    """

    def __init__(self, username: str = "nhagar", suffix: str = "_urls"):
        """
        Initialize HuggingFace loader.

        Args:
            username: HuggingFace username (default: 'nhagar')
            suffix: Dataset name suffix (default: '_urls')
        """
        self.username = username
        self.suffix = suffix

    def load(
        self,
        dataset_name: str,
        split: str = "train",
        streaming: bool = True,
    ) -> Iterator[pl.DataFrame]:
        """
        Load a dataset from HuggingFace Hub.

        Args:
            dataset_name: Dataset name (without username prefix or suffix)
            split: Dataset split to load (default: 'train')
            streaming: Use streaming mode (default: True for large datasets)

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
                streaming=streaming,
            )

            if streaming:
                # Yield batches from streaming dataset
                yield from self._stream_batches(dataset)
            else:
                # Convert entire dataset to Polars DataFrame
                df = pl.from_arrow(dataset.data.table)
                yield df

        except Exception as e:
            raise ValueError(f"Failed to load dataset '{full_name}': {e}")

    def _stream_batches(
        self, dataset, batch_size: int = 10000
    ) -> Iterator[pl.DataFrame]:
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

    def load_parquet_files(
        self, dataset_name: str, cache_dir: Optional[str] = None
    ) -> list[str]:
        """
        Load dataset and get paths to cached Parquet files.

        This is useful when you want to work directly with Parquet files
        instead of streaming records.

        Args:
            dataset_name: Dataset name (without username prefix or suffix)
            cache_dir: Optional cache directory

        Returns:
            List of paths to Parquet files

        Raises:
            ValueError: If dataset cannot be loaded
        """
        full_name = f"{self.username}/{dataset_name}{self.suffix}"

        try:
            # Load dataset (non-streaming to get all files)
            dataset = load_dataset(
                full_name,
                split="train",
                streaming=False,
                cache_dir=cache_dir,
            )

            # Get Parquet file paths from cache
            # HuggingFace datasets are stored as Arrow/Parquet files
            if hasattr(dataset, "cache_files"):
                return [f["filename"] for f in dataset.cache_files]

            # Fallback: dataset might not have cache_files attribute
            return []

        except Exception as e:
            raise ValueError(f"Failed to load dataset '{full_name}': {e}")

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
