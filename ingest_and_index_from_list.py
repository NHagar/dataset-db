#!/usr/bin/env python3
"""
Batch ingestion + index building from a dataset list.

Reads dataset names from a text file, ingests each dataset from the HuggingFace
Hub with resume support, and then builds/updates the index incrementally.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional

# Ensure local package imports work when running as a script
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dataset_db.config import get_config
from dataset_db.index import IndexBuilder
from dataset_db.ingestion import HuggingFaceLoader, IngestionProcessor
from dataset_db.ingestion.dataset_registry import DatasetRegistry
from dataset_db.storage import ParquetWriter

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    config = get_config()
    parser = argparse.ArgumentParser(
        description="Ingest multiple datasets and build/update the index."
    )
    parser.add_argument(
        "--dataset-file",
        required=True,
        type=Path,
        help="Path to a file containing dataset names (one per line, '#' comments allowed).",
    )
    parser.add_argument(
        "--username",
        default=config.ingestion.hf_username,
        help="HuggingFace username prefix (defaults to config).",
    )
    parser.add_argument(
        "--base-path",
        type=Path,
        default=config.storage.base_path,
        help="Base path for data and indexes (defaults to config.storage.base_path).",
    )
    parser.add_argument(
        "--split",
        default="train",
        help="Dataset split to stream from HuggingFace (default: train).",
    )
    parser.add_argument(
        "--resume",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Resume ingestion from saved state (default: on).",
    )
    parser.add_argument(
        "--dataset-suffix",
        default=None,
        help="Optional suffix to append when missing (e.g., _urls).",
    )
    parser.add_argument(
        "--skip-existing",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Skip datasets that already have ingested Parquet data (default: on).",
    )
    parser.add_argument(
        "--force-reingest",
        action="store_true",
        help="Force re-ingestion even if data already exists (overrides --skip-existing).",
    )
    parser.add_argument(
        "--postings-shards",
        type=int,
        default=config.index.postings_shards,
        help="Number of postings shards for the index builder.",
    )
    return parser.parse_args()


def load_dataset_names(
    path: Path, username: str, suffix: Optional[str]
) -> list[str]:
    """
    Read dataset names from file, normalizing entries.

    - Ignores blank lines and comments starting with '#'
    - Allows entries with or without username prefix (user/dataset); username argument wins
    - Optionally appends suffix when provided and missing
    """
    if not path.exists():
        raise FileNotFoundError(f"Dataset list file not found: {path}")

    names: list[str] = []
    for raw_line in path.read_text().splitlines():
        entry = raw_line.strip()
        if not entry or entry.startswith("#"):
            continue

        if "/" in entry:
            entry_username, dataset = entry.split("/", 1)
            if entry_username != username:
                logger.warning(
                    "Dataset entry '%s' uses username '%s'; overriding with '%s'",
                    entry,
                    entry_username,
                    username,
                )
            entry = dataset

        if suffix and not entry.endswith(suffix):
            entry = f"{entry}{suffix}"

        names.append(entry)

    if not names:
        raise ValueError(f"No dataset names found in {path}")

    return names


def ingest_dataset(
    dataset_name: str,
    loader: HuggingFaceLoader,
    processor: IngestionProcessor,
    writer: ParquetWriter,
    split: str,
    resume: bool,
) -> Optional[int]:
    """
    Ingest a single dataset with resume support.

    Returns the dataset_id on success, or None on failure.
    """
    logger.info("=== Ingesting dataset '%s' (resume=%s) ===", dataset_name, resume)
    batch_count = 0
    total_rows = 0
    start_time = time.time()

    try:
        for batch_df in loader.load(dataset_name, split=split, resume=resume):
            loader.validate_schema(batch_df)
            normalized_df = processor.process_batch(batch_df, dataset_name)

            result = writer.write_batch(normalized_df)
            batch_count += 1
            total_rows += result["total_rows_processed"]
            logger.info(
                "Batch %d: %s rows processed (buffered=%s, flushed=%s, files=%s)",
                batch_count,
                result["total_rows_processed"],
                result["rows_buffered"],
                result["rows_flushed"],
                result["files_written"],
            )

            # Persist state so we can resume mid-dataset
            loader.save_state_dict(dataset_name)

        flush_result = writer.flush()
        writer_stats = writer.get_stats()
        dataset_id = processor.dataset_registry.get_dataset_id(dataset_name)

        loader.clear_state_dict(dataset_name)

        elapsed = time.time() - start_time
        logger.info(
            "Completed dataset '%s' (dataset_id=%s) in %.2fs | rows=%s | "
            "flush_rows=%s | files_created=%s",
            dataset_name,
            dataset_id,
            elapsed,
            total_rows,
            flush_result["rows_written"],
            writer_stats["files_created"],
        )
        return dataset_id

    except Exception:
        logger.exception(
            "Ingestion failed for dataset '%s'; state retained for resume", dataset_name
        )
        return None


def dataset_already_ingested(
    dataset_name: str,
    dataset_registry: DatasetRegistry,
    writer: ParquetWriter,
    loader: HuggingFaceLoader,
) -> bool:
    """
    Check if a dataset already has ingested Parquet data and no pending resume state.

    A dataset is considered "already ingested" if:
    - It exists in the dataset registry
    - It has at least one partition on disk
    - There is no saved state dict (which would indicate an incomplete ingestion)
    """
    registry_map = dataset_registry.to_dict()
    if dataset_name not in registry_map:
        return False

    dataset_id = registry_map[dataset_name]
    layout = writer.layout

    # Pending resume state means ingestion is not complete
    if loader.get_state_dict_path(dataset_name).exists():
        return False

    partitions = layout.list_partitions(dataset_id=dataset_id)
    return len(partitions) > 0


def build_index(
    base_path: Path, dataset_ids: Iterable[int], postings_shards: int
) -> str:
    """Run the incremental index build for the ingested datasets."""
    builder = IndexBuilder(base_path=base_path, num_postings_shards=postings_shards)
    dataset_id_list: List[int] = list(dataset_ids)
    version = builder.build_incremental(dataset_ids=dataset_id_list or None)
    logger.info(
        "Published index version %s (datasets=%s)",
        version,
        dataset_id_list if dataset_id_list else "all",
    )
    return version


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    base_path = args.base_path.resolve()
    config = get_config()
    config.storage.base_path = base_path

    dataset_names = load_dataset_names(args.dataset_file, args.username, args.dataset_suffix)
    logger.info("Datasets to ingest (%d): %s", len(dataset_names), dataset_names)

    # Shared components
    dataset_registry = DatasetRegistry(base_path=base_path)
    processor = IngestionProcessor(dataset_registry=dataset_registry)
    loader = HuggingFaceLoader(username=args.username)
    writer = ParquetWriter(base_path=base_path)

    ingested_ids: list[int] = []
    for name in dataset_names:
        if args.force_reingest:
            logger.info("Force re-ingest enabled for dataset '%s'", name)
        elif args.skip_existing and dataset_already_ingested(
            name, dataset_registry, writer, loader
        ):
            ds_id = dataset_registry.get_dataset_id(name)
            logger.info(
                "Skipping dataset '%s' (dataset_id=%s) - already ingested",
                name,
                ds_id,
            )
            ingested_ids.append(ds_id)
            continue

        ds_id = ingest_dataset(
            dataset_name=name,
            loader=loader,
            processor=processor,
            writer=writer,
            split=args.split,
            resume=args.resume,
        )
        if ds_id is not None:
            ingested_ids.append(ds_id)

    if not ingested_ids:
        logger.error("No datasets were ingested successfully; skipping index build.")
        sys.exit(1)

    version = build_index(
        base_path=base_path,
        dataset_ids=ingested_ids,
        postings_shards=args.postings_shards,
    )
    logger.info("Index build complete: version %s", version)


if __name__ == "__main__":
    main()
