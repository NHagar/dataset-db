"""
Storage layout management for partitioned Parquet files.

Implements the directory structure from spec.md §2.1:
    s3://{bucket}/urls/
      dataset_id={id}/
        domain_prefix={hh}/
          part-00000.parquet
          part-00001.parquet
"""

from pathlib import Path
from typing import Optional


class StorageLayout:
    """
    Manages the partitioned directory structure for Parquet files.

    Implements the layout specified in spec.md §2.1 with Hive-style partitioning
    by dataset_id and domain_prefix.
    """

    def __init__(self, base_path: Path):
        """
        Initialize storage layout manager.

        Args:
            base_path: Root directory for all data (e.g., ./data or s3://bucket)
        """
        self.base_path = Path(base_path)
        self.urls_root = self.base_path / "urls"

    def get_partition_path(
        self, dataset_id: int, domain_prefix: str
    ) -> Path:
        """
        Get the directory path for a specific partition.

        Args:
            dataset_id: Dataset identifier
            domain_prefix: Domain prefix (e.g., "3a" from first 2 hex chars)

        Returns:
            Path to partition directory

        Example:
            >>> layout = StorageLayout(Path("./data"))
            >>> layout.get_partition_path(17, "3a")
            PosixPath('data/urls/dataset_id=17/domain_prefix=3a')
        """
        return (
            self.urls_root
            / f"dataset_id={dataset_id}"
            / f"domain_prefix={domain_prefix}"
        )

    def get_parquet_path(
        self,
        dataset_id: int,
        domain_prefix: str,
        part_number: int,
    ) -> Path:
        """
        Get the full path for a specific Parquet file.

        Args:
            dataset_id: Dataset identifier
            domain_prefix: Domain prefix (e.g., "3a")
            part_number: Part file number (0-based)

        Returns:
            Path to Parquet file

        Example:
            >>> layout = StorageLayout(Path("./data"))
            >>> layout.get_parquet_path(17, "3a", 0)
            PosixPath('data/urls/dataset_id=17/domain_prefix=3a/part-00000.parquet')
        """
        partition_path = self.get_partition_path(dataset_id, domain_prefix)
        return partition_path / f"part-{part_number:05d}.parquet"

    def ensure_partition_exists(
        self, dataset_id: int, domain_prefix: str
    ) -> Path:
        """
        Create partition directory if it doesn't exist.

        Args:
            dataset_id: Dataset identifier
            domain_prefix: Domain prefix

        Returns:
            Path to partition directory
        """
        partition_path = self.get_partition_path(dataset_id, domain_prefix)
        partition_path.mkdir(parents=True, exist_ok=True)
        return partition_path

    def list_partitions(
        self, dataset_id: Optional[int] = None
    ) -> list[tuple[int, str]]:
        """
        List all partitions, optionally filtered by dataset_id.

        Args:
            dataset_id: Optional dataset ID to filter by

        Returns:
            List of (dataset_id, domain_prefix) tuples

        Example:
            >>> layout = StorageLayout(Path("./data"))
            >>> layout.list_partitions(dataset_id=17)
            [(17, '3a'), (17, '5b'), (17, 'ff')]
        """
        partitions = []

        if not self.urls_root.exists():
            return partitions

        # Iterate over dataset_id directories
        for dataset_dir in self.urls_root.iterdir():
            if not dataset_dir.is_dir():
                continue

            # Parse dataset_id from directory name
            if not dataset_dir.name.startswith("dataset_id="):
                continue

            try:
                ds_id = int(dataset_dir.name.split("=")[1])
            except (IndexError, ValueError):
                continue

            # Filter by dataset_id if specified
            if dataset_id is not None and ds_id != dataset_id:
                continue

            # Iterate over domain_prefix directories
            for prefix_dir in dataset_dir.iterdir():
                if not prefix_dir.is_dir():
                    continue

                if not prefix_dir.name.startswith("domain_prefix="):
                    continue

                try:
                    prefix = prefix_dir.name.split("=")[1]
                    # Skip empty prefixes
                    if not prefix:
                        continue
                    partitions.append((ds_id, prefix))
                except IndexError:
                    continue

        return sorted(partitions)

    def list_parquet_files(
        self, dataset_id: int, domain_prefix: str
    ) -> list[Path]:
        """
        List all Parquet files in a specific partition.

        Args:
            dataset_id: Dataset identifier
            domain_prefix: Domain prefix

        Returns:
            List of Parquet file paths, sorted by part number
        """
        partition_path = self.get_partition_path(dataset_id, domain_prefix)

        if not partition_path.exists():
            return []

        # Find all .parquet files
        parquet_files = sorted(partition_path.glob("part-*.parquet"))
        return parquet_files

    def get_next_part_number(
        self, dataset_id: int, domain_prefix: str
    ) -> int:
        """
        Get the next available part number for a partition.

        Args:
            dataset_id: Dataset identifier
            domain_prefix: Domain prefix

        Returns:
            Next part number (0 if no files exist)
        """
        existing_files = self.list_parquet_files(dataset_id, domain_prefix)

        if not existing_files:
            return 0

        # Extract part numbers and find max
        part_numbers = []
        for file_path in existing_files:
            # Parse "part-00000.parquet" -> 0
            try:
                part_str = file_path.stem.split("-")[1]
                part_num = int(part_str)
                part_numbers.append(part_num)
            except (IndexError, ValueError):
                continue

        return max(part_numbers, default=-1) + 1

    def get_stats(self) -> dict:
        """
        Get storage statistics.

        Returns:
            Dictionary with storage stats
        """
        if not self.urls_root.exists():
            return {
                "total_partitions": 0,
                "total_files": 0,
                "total_size_bytes": 0,
                "datasets": [],
            }

        partitions = self.list_partitions()
        total_files = 0
        total_size = 0
        dataset_stats = {}

        for dataset_id, domain_prefix in partitions:
            files = self.list_parquet_files(dataset_id, domain_prefix)
            total_files += len(files)

            for file_path in files:
                if file_path.exists():
                    total_size += file_path.stat().st_size

            if dataset_id not in dataset_stats:
                dataset_stats[dataset_id] = {
                    "dataset_id": dataset_id,
                    "partitions": 0,
                    "files": 0,
                }

            dataset_stats[dataset_id]["partitions"] += 1
            dataset_stats[dataset_id]["files"] += len(files)

        return {
            "total_partitions": len(partitions),
            "total_files": total_files,
            "total_size_bytes": total_size,
            "datasets": list(dataset_stats.values()),
        }
