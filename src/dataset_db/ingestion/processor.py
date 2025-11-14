"""
Ingestion processor pipeline.

Processes raw URL datasets through normalization and prepares for Parquet storage.
"""

from typing import Optional

import polars as pl

from dataset_db.config import get_config
from dataset_db.normalization import IDGenerator, URLNormalizer

from .dataset_registry import DatasetRegistry


class IngestionProcessor:
    """
    Process raw URL data through normalization pipeline.

    Transforms input records with {url, domain} into normalized format ready
    for Parquet storage with schema from spec.md §2.1.
    """

    def __init__(
        self,
        normalizer: Optional[URLNormalizer] = None,
        id_generator: Optional[IDGenerator] = None,
        dataset_registry: Optional[DatasetRegistry] = None,
    ):
        """
        Initialize ingestion processor.

        Args:
            normalizer: URL normalizer instance (creates new if None)
            id_generator: ID generator instance (creates new if None)
            dataset_registry: Persistent dataset registry (creates new if None)
        """
        self.normalizer = normalizer or URLNormalizer()
        self.id_generator = id_generator or IDGenerator()
        self.dataset_registry = dataset_registry or DatasetRegistry()
        self.config = get_config()

    def process_batch(self, df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
        """
        Process a batch of URLs through normalization pipeline.

        Input schema (from HuggingFace):
            - url: string
            - domain: string (may be ignored, we extract our own)

        Output schema (spec.md §2.1):
            - dataset_id: Int32
            - domain_id: Int64
            - url_id: Int64
            - scheme: String (DICTIONARY encoded in Parquet)
            - host: String (DICTIONARY encoded in Parquet)
            - path_query: String (DICTIONARY encoded in Parquet)
            - domain: String (normalized eTLD+1, DICTIONARY encoded)

        Args:
            df: Input Polars DataFrame with 'url' column
            dataset_name: Name of the dataset being processed

        Returns:
            Normalized DataFrame ready for Parquet writing
        """
        # Register dataset and get persistent ID
        dataset_id = self.dataset_registry.register_dataset(dataset_name)

        # Process each URL through normalizer
        normalized_records = []

        for row in df.iter_rows(named=True):
            raw_url = row.get("url", "")

            if not raw_url:
                continue  # Skip empty URLs

            try:
                norm = self.normalizer.normalize(raw_url)

                # Generate IDs
                url_id = self.id_generator.get_url_id(raw_url)
                domain_id = self.id_generator.get_domain_id(norm.domain)

                normalized_records.append(
                    {
                        "dataset_id": dataset_id,
                        "domain_id": domain_id,
                        "url_id": url_id,
                        "scheme": norm.scheme,
                        "host": norm.host,
                        "path_query": norm.get_path_query(),
                        "domain": norm.domain,
                        "domain_prefix": self.id_generator.get_domain_prefix(
                            norm.domain,
                            self.config.storage.domain_prefix_chars,
                        ),
                    }
                )

            except (ValueError, Exception) as e:
                # Log error but continue processing
                # In production, you'd want proper logging here
                print(f"Warning: Failed to normalize URL '{raw_url}': {e}")
                continue

        # Convert to Polars DataFrame with proper types
        if not normalized_records:
            # Return empty DataFrame with correct schema
            return self._empty_dataframe()

        result_df = pl.DataFrame(normalized_records)

        # Ensure correct types
        result_df = result_df.with_columns(
            [
                pl.col("dataset_id").cast(pl.Int32),
                pl.col("domain_id").cast(pl.Int64),
                pl.col("url_id").cast(pl.Int64),
                pl.col("scheme").cast(pl.Utf8),
                pl.col("host").cast(pl.Utf8),
                pl.col("path_query").cast(pl.Utf8),
                pl.col("domain").cast(pl.Utf8),
                pl.col("domain_prefix").cast(pl.Utf8),
            ]
        )

        return result_df

    def _empty_dataframe(self) -> pl.DataFrame:
        """Create empty DataFrame with correct schema."""
        return pl.DataFrame(
            schema={
                "dataset_id": pl.Int32,
                "domain_id": pl.Int64,
                "url_id": pl.Int64,
                "scheme": pl.Utf8,
                "host": pl.Utf8,
                "path_query": pl.Utf8,
                "domain": pl.Utf8,
                "domain_prefix": pl.Utf8,
            }
        )

    def get_stats(self) -> dict:
        """
        Get processing statistics.

        Returns:
            Dictionary with stats like datasets processed, URL counts, etc.
        """
        datasets = self.dataset_registry.to_dict()
        return {
            "datasets_processed": len(datasets),
            "datasets": datasets,
        }

    @staticmethod
    def reconstruct_url(scheme: str, host: str, path_query: str) -> str:
        """
        Reconstruct normalized URL from components.

        Since we don't store raw_url, this utility reconstructs the
        normalized URL from its components.

        Args:
            scheme: URL scheme (e.g., 'https')
            host: Hostname (e.g., 'example.com')
            path_query: Path and query string (e.g., '/path?a=1')

        Returns:
            Reconstructed URL

        Example:
            >>> reconstruct_url('https', 'example.com', '/path?a=1')
            'https://example.com/path?a=1'
        """
        return f"{scheme}://{host}{path_query}"
