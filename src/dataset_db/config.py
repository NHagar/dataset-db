"""
Configuration management for the dataset-db system.
"""

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestionConfig(BaseSettings):
    """Configuration for the ingestion pipeline."""

    # HuggingFace dataset settings
    hf_username: str = Field(default="nhagar", description="HuggingFace username")
    dataset_name_suffix: str = Field(
        default="_urls", description="Suffix for dataset names"
    )

    # Processing settings
    batch_size: int = Field(
        default=1_000_000,
        description="Batch size for streaming from HuggingFace (rows per batch)",
    )
    max_workers: int = Field(
        default=4, description="Number of parallel workers for processing"
    )

    # Parquet settings
    row_group_size: int = Field(
        default=128 * 1024 * 1024,  # 128MB
        description="Target row group size in bytes",
    )
    partition_buffer_size: int = Field(
        default=128 * 1024 * 1024,  # 128MB
        description="Buffer size per partition before flushing to disk",
    )
    max_total_buffer_size: int = Field(
        default=1 * 1024 * 1024 * 1024,  # 1GB
        description=(
            "Global in-memory cap across all partitions before forced flush. "
            "Set to 0 to disable."
        ),
    )
    compression: str = Field(default="zstd", description="Compression codec")
    compression_level: int = Field(
        default=6, description="Compression level (1-22 for zstd)"
    )

    model_config = SettingsConfigDict(env_prefix="INGEST_")


class StorageConfig(BaseSettings):
    """Configuration for storage layer."""

    # Local or S3 storage
    storage_type: str = Field(
        default="local", description="Storage backend: 'local' or 's3'"
    )
    base_path: Path = Field(
        default=Path("./data"), description="Base path for local storage"
    )

    # S3 settings (if storage_type == 's3')
    s3_bucket: Optional[str] = Field(default=None, description="S3 bucket name")
    s3_region: Optional[str] = Field(default="us-east-1", description="S3 region")

    # Partitioning
    domain_prefix_chars: int = Field(
        default=2, description="Number of hex chars for domain prefix partitioning"
    )

    model_config = SettingsConfigDict(env_prefix="STORAGE_")


class IndexConfig(BaseSettings):
    """Configuration for index building."""

    # Sharding
    postings_shards: int = Field(
        default=1024, description="Number of shards for postings"
    )

    # MPHF settings
    mphf_gamma: float = Field(
        default=2.0, description="BBHash gamma parameter (space/time tradeoff)"
    )

    model_config = SettingsConfigDict(env_prefix="INDEX_")


class Config(BaseSettings):
    """Main configuration."""

    # Sub-configs
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    index: IndexConfig = Field(default_factory=IndexConfig)

    # Global settings
    log_level: str = Field(default="INFO", description="Logging level")

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_nested_delimiter="__"
    )


# Global config instance
_config: Optional[Config] = None


def get_config() -> Config:
    """Get or create the global configuration instance."""
    global _config
    if _config is None:
        _config = Config()
    return _config


def reset_config() -> None:
    """Reset the global configuration (mainly for testing)."""
    global _config
    _config = None
