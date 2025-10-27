"""
Query service for domain and URL lookups.

Implements the query algorithm per spec.md §4.3.
"""

import logging
from pathlib import Path

import polars as pl

from dataset_db.api.loader import IndexLoader
from dataset_db.api.models import DatasetInfo, DomainResponse, URLItem, URLsResponse

logger = logging.getLogger(__name__)


class QueryService:
    """
    Service for executing domain and URL queries.

    Implements the query algorithms from spec.md §4.3.
    """

    def __init__(self, loader: IndexLoader):
        """
        Initialize the query service.

        Args:
            loader: IndexLoader instance with loaded indexes
        """
        self.loader = loader

    def get_datasets_for_domain(self, domain: str) -> DomainResponse:
        """
        Get list of datasets containing the given domain.

        Implements spec.md §4.3 get_datasets_for_domain algorithm.

        Args:
            domain: Domain string (will be normalized)

        Returns:
            DomainResponse with datasets and counts

        Raises:
            ValueError: If domain not found
        """
        # Lookup domain ID via MPHF
        domain_id = self.loader.lookup_domain_id(domain)
        if domain_id is None:
            raise ValueError(f"Domain not found: {domain}")

        # Fetch datasets from membership index (Roaring bitmap)
        dataset_ids = self.loader.get_datasets_for_domain(domain_id)

        # Build response (counts will be added in future milestone)
        datasets = [DatasetInfo(dataset_id=ds_id, url_count_est=None) for ds_id in dataset_ids]

        return DomainResponse(domain=domain, domain_id=domain_id, datasets=datasets)

    def get_urls_for_domain_dataset(
        self, domain: str, dataset_id: int, offset: int = 0, limit: int = 1000
    ) -> URLsResponse:
        """
        Get URLs for a given (domain, dataset) pair with pagination.

        Implements spec.md §4.3 get_urls_for_domain_dataset algorithm.

        Args:
            domain: Domain string (will be normalized)
            dataset_id: Dataset ID
            offset: Offset for pagination (default: 0)
            limit: Maximum number of URLs to return (default: 1000)

        Returns:
            URLsResponse with paginated URLs

        Raises:
            ValueError: If domain not found or dataset doesn't contain domain
        """
        # Lookup domain ID
        domain_id = self.loader.lookup_domain_id(domain)
        if domain_id is None:
            raise ValueError(f"Domain not found: {domain}")

        # Check that dataset contains this domain
        dataset_ids = self.loader.get_datasets_for_domain(domain_id)
        if dataset_id not in dataset_ids:
            raise ValueError(
                f"Dataset {dataset_id} does not contain domain {domain} (domain_id={domain_id})"
            )

        # Lookup postings for (domain_id, dataset_id)
        postings_entry = self.loader.postings.lookup(domain_id, dataset_id)
        if postings_entry is None:
            # No postings found (should not happen if membership index is consistent)
            logger.warning(
                f"No postings found for domain_id={domain_id}, dataset_id={dataset_id}"
            )
            return URLsResponse(
                domain=domain, dataset_id=dataset_id, total_est=0, items=[], next_offset=None
            )

        # Decode postings payload to get list of (file_id, row_group) tuples
        row_group_refs = postings_entry.row_groups

        # Calculate which row groups we need to read
        urls = []
        current_offset = 0
        remaining = limit

        for file_id, row_group in row_group_refs:
            # Get file metadata from registry
            file_info = self.loader.file_registry.get_file(file_id)
            if file_info is None:
                logger.warning(f"File {file_id} not found in registry, skipping")
                continue

            # Read the Parquet row group
            parquet_path = self.loader.base_path / file_info.path
            if not parquet_path.exists():
                logger.warning(f"Parquet file not found: {parquet_path}, skipping")
                continue

            # Read specific row group, filtering by domain_id
            try:
                df = self._read_row_group_filtered(parquet_path, row_group, domain_id)
            except Exception as e:
                logger.error(f"Error reading {parquet_path} row_group {row_group}: {e}")
                continue

            # Handle offset within this row group
            if current_offset < offset:
                skip = min(offset - current_offset, len(df))
                df = df[skip:]
                current_offset += skip

            # Take up to 'remaining' rows
            if len(df) > remaining:
                df = df[:remaining]

            # Convert to URL items
            batch_urls = self._df_to_url_items(df)
            urls.extend(batch_urls)

            current_offset += len(df)
            remaining -= len(batch_urls)

            if remaining <= 0:
                break

        # Determine next offset
        next_offset = offset + len(urls) if len(urls) == limit else None

        return URLsResponse(
            domain=domain,
            dataset_id=dataset_id,
            total_est=None,  # TODO: Add pre-aggregated counts in future milestone
            items=urls,
            next_offset=next_offset,
        )

    def _read_row_group_filtered(
        self, parquet_path: Path, row_group: int, domain_id: int
    ) -> pl.DataFrame:
        """
        Read a specific row group from a Parquet file, filtering by domain_id.

        Args:
            parquet_path: Path to Parquet file
            row_group: Row group number
            domain_id: Domain ID to filter on

        Returns:
            Filtered DataFrame
        """
        # Read specific row group with filter
        # Note: Polars doesn't directly support row group reading, so we read the whole file
        # and filter. In production, we'd use PyArrow or DuckDB for row-group-level reads.
        df = pl.scan_parquet(parquet_path).filter(pl.col("domain_id") == domain_id).collect()

        return df

    def _df_to_url_items(self, df: pl.DataFrame) -> list[URLItem]:
        """
        Convert DataFrame rows to URLItem objects.

        Reconstructs full URLs from (scheme, host, path_query) components.

        Args:
            df: DataFrame with url_id, scheme, host, path_query columns

        Returns:
            List of URLItem objects
        """
        items = []

        for row in df.iter_rows(named=True):
            # Reconstruct URL from components
            scheme = row.get("scheme", "https")
            host = row.get("host", "")
            path_query = row.get("path_query", "/")

            # Handle missing scheme (shouldn't happen, but be defensive)
            if not scheme:
                scheme = "https"

            url = f"{scheme}://{host}{path_query}"

            items.append(
                URLItem(
                    url_id=row["url_id"],
                    url=url,
                    ts=row.get("ts"),  # Optional timestamp
                )
            )

        return items
