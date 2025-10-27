"""
File registry for mapping file IDs to Parquet file paths.

As described in spec.md §2.2C, this is a simple TSV mapping:
file_id \t dataset_id \t domain_prefix \t parquet_rel_path
"""

import io
import logging
from pathlib import Path

import polars as pl
import zstandard as zstd

from ..storage.layout import StorageLayout

logger = logging.getLogger(__name__)


class FileRegistry:
    """
    Manage file ID to Parquet path mappings.
    """

    def __init__(self, base_path: Path):
        """
        Initialize file registry.

        Args:
            base_path: Base path for storage
        """
        self.base_path = Path(base_path)
        self.layout = StorageLayout(base_path)
        self.files: list[dict[str, str | int]] = []
        self.path_to_id: dict[str, int] = {}

    def scan_parquet_files(self) -> None:
        """
        Scan all Parquet files and assign file IDs.
        """
        logger.info("Scanning Parquet files...")

        urls_dir = self.base_path / "urls"
        if not urls_dir.exists():
            logger.warning(f"URLs directory does not exist: {urls_dir}")
            return

        parquet_files = sorted(urls_dir.rglob("*.parquet"))
        if not parquet_files:
            logger.warning("No Parquet files found")
            return

        logger.info(f"Found {len(parquet_files)} Parquet files")

        for file_id, parquet_file in enumerate(parquet_files):
            # Parse dataset_id and domain_prefix from path
            parts = parquet_file.parts

            dataset_id = None
            domain_prefix = None

            for part in parts:
                if part.startswith("dataset_id="):
                    dataset_id = int(part.split("=")[1])
                elif part.startswith("domain_prefix="):
                    domain_prefix = part.split("=")[1]

            if dataset_id is None or domain_prefix is None:
                logger.warning(
                    f"Could not extract dataset_id/domain_prefix from {parquet_file}"
                )
                continue

            # Get relative path from urls/ directory
            rel_path = str(parquet_file.relative_to(urls_dir))

            # Add to registry
            self.files.append(
                {
                    "file_id": file_id,
                    "dataset_id": dataset_id,
                    "domain_prefix": domain_prefix,
                    "parquet_rel_path": rel_path,
                }
            )

            self.path_to_id[rel_path] = file_id

        logger.info(f"Registered {len(self.files)} Parquet files")

    def save(self, output_path: Path, compression_level: int = 6) -> None:
        """
        Save file registry to TSV with compression.

        Args:
            output_path: Path to save registry
            compression_level: Zstd compression level
        """
        logger.info(f"Saving file registry to {output_path}...")

        if not self.files:
            logger.warning("No files in registry")
            return

        # Convert to DataFrame
        df = pl.DataFrame(self.files)

        # Write to TSV via StringIO buffer
        buffer = io.StringIO()
        df.write_csv(file=buffer, separator="\t")
        tsv_bytes = buffer.getvalue().encode("utf-8")

        # Compress
        compressor = zstd.ZstdCompressor(level=compression_level)
        compressed_data = compressor.compress(tsv_bytes)

        # Write
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(compressed_data)

        # Log statistics
        original_size = len(tsv_bytes)
        compressed_size = len(compressed_data)
        ratio = original_size / compressed_size if compressed_size > 0 else 0

        logger.info(
            f"Saved file registry: {len(self.files)} files, "
            f"{original_size:,} bytes → {compressed_size:,} bytes "
            f"(compression ratio: {ratio:.2f}x)"
        )

    def load(self, input_path: Path) -> None:
        """
        Load file registry from TSV.

        Args:
            input_path: Path to registry file
        """
        logger.info(f"Loading file registry from {input_path}...")

        if not input_path.exists():
            raise FileNotFoundError(f"File registry not found: {input_path}")

        # Decompress
        decompressor = zstd.ZstdDecompressor()
        compressed_data = input_path.read_bytes()
        tsv_bytes = decompressor.decompress(compressed_data)

        # Parse TSV
        df = pl.read_csv(tsv_bytes, separator="\t")

        # Convert to list of dicts
        self.files = df.to_dicts()

        # Build reverse lookup
        self.path_to_id = {
            row["parquet_rel_path"]: row["file_id"] for row in self.files
        }

        logger.info(f"Loaded file registry: {len(self.files)} files")

    def get_file_path(self, file_id: int) -> str | None:
        """
        Get Parquet file path by file ID.

        Args:
            file_id: File ID to look up

        Returns:
            Relative path to Parquet file, or None if not found
        """
        if file_id < 0 or file_id >= len(self.files):
            return None
        return self.files[file_id]["parquet_rel_path"]

    def get_file_info(self, file_id: int) -> dict[str, str | int] | None:
        """
        Get complete file info by file ID.

        Args:
            file_id: File ID to look up

        Returns:
            Dict with file_id, dataset_id, domain_prefix, parquet_rel_path
        """
        if file_id < 0 or file_id >= len(self.files):
            return None
        return self.files[file_id]

    def get_file_id(self, rel_path: str) -> int | None:
        """
        Get file ID by relative path.

        Args:
            rel_path: Relative path to Parquet file

        Returns:
            File ID, or None if not found
        """
        return self.path_to_id.get(rel_path)

    def build(self, version: str, base_path: Path) -> Path:
        """
        Build complete file registry and save to disk.

        Args:
            version: Version identifier
            base_path: Base path for storage

        Returns:
            Path to saved file registry
        """
        self.scan_parquet_files()

        output_path = base_path / "index" / version / "files.tsv.zst"
        self.save(output_path)

        return output_path

    def build_incremental(
        self, version: str, base_path: Path, prev_registry_path: Path | None = None
    ) -> Path:
        """
        Build file registry incrementally by merging with previous version.

        Args:
            version: Version identifier
            base_path: Base path for storage
            prev_registry_path: Path to previous registry file (optional)

        Returns:
            Path to saved file registry
        """
        logger.info("Building file registry incrementally...")

        # Load previous registry if provided
        existing_files = []
        next_file_id = 0

        if prev_registry_path and prev_registry_path.exists():
            logger.info(f"Loading previous registry from {prev_registry_path}")
            self.load(prev_registry_path)
            existing_files = self.files.copy()

            # Find max file_id to start numbering new files
            if existing_files:
                next_file_id = max(f["file_id"] for f in existing_files) + 1

            logger.info(
                f"Loaded {len(existing_files)} existing files, "
                f"next file_id will be {next_file_id}"
            )

        # Scan all Parquet files
        urls_dir = self.base_path / "urls"
        if not urls_dir.exists():
            logger.warning(f"URLs directory does not exist: {urls_dir}")
            return base_path / "index" / version / "files.tsv.zst"

        all_parquet_files = sorted(urls_dir.rglob("*.parquet"))
        logger.info(f"Found {len(all_parquet_files)} total Parquet files")

        # Determine which files are new
        existing_paths = {f["parquet_rel_path"] for f in existing_files}
        new_files = []

        for parquet_file in all_parquet_files:
            rel_path = str(parquet_file.relative_to(urls_dir))

            if rel_path in existing_paths:
                continue  # Already registered

            # Parse dataset_id and domain_prefix from path
            parts = parquet_file.parts

            dataset_id = None
            domain_prefix = None

            for part in parts:
                if part.startswith("dataset_id="):
                    dataset_id = int(part.split("=")[1])
                elif part.startswith("domain_prefix="):
                    domain_prefix = part.split("=")[1]

            if dataset_id is None or domain_prefix is None:
                logger.warning(
                    f"Could not extract dataset_id/domain_prefix from {parquet_file}"
                )
                continue

            # Add to new files list
            new_files.append(
                {
                    "file_id": next_file_id,
                    "dataset_id": dataset_id,
                    "domain_prefix": domain_prefix,
                    "parquet_rel_path": rel_path,
                }
            )
            next_file_id += 1

        logger.info(f"Found {len(new_files)} new files to register")

        # Merge existing + new files
        self.files = existing_files + new_files

        # Rebuild reverse lookup
        self.path_to_id = {row["parquet_rel_path"]: row["file_id"] for row in self.files}

        # Save merged registry
        output_path = base_path / "index" / version / "files.tsv.zst"
        self.save(output_path)

        logger.info(
            f"Built incremental file registry: "
            f"{len(existing_files)} existing + {len(new_files)} new = {len(self.files)} total"
        )

        return output_path

    def get_new_files_since_version(
        self, prev_registry_path: Path | None
    ) -> list[Path]:
        """
        Determine which Parquet files are new since the previous version.

        Args:
            prev_registry_path: Path to previous registry file

        Returns:
            List of absolute paths to new Parquet files
        """
        logger.info("Determining new files since previous version...")

        # Load previous registry
        existing_paths = set()
        if prev_registry_path and prev_registry_path.exists():
            prev_registry = FileRegistry(self.base_path)
            prev_registry.load(prev_registry_path)
            existing_paths = {f["parquet_rel_path"] for f in prev_registry.files}
            logger.info(f"Previous version had {len(existing_paths)} files")

        # Scan all current Parquet files
        urls_dir = self.base_path / "urls"
        if not urls_dir.exists():
            logger.warning(f"URLs directory does not exist: {urls_dir}")
            return []

        all_parquet_files = list(urls_dir.rglob("*.parquet"))
        logger.info(f"Current version has {len(all_parquet_files)} files")

        # Filter to only new files
        new_files = []
        for parquet_file in all_parquet_files:
            rel_path = str(parquet_file.relative_to(urls_dir))
            if rel_path not in existing_paths:
                new_files.append(parquet_file)

        logger.info(f"Found {len(new_files)} new files")

        return new_files
