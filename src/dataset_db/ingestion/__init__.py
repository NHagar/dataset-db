"""
Data ingestion pipeline.

Handles loading from HuggingFace datasets and processing into normalized format.
"""

from .dataset_registry import DatasetRegistry
from .hf_loader import HuggingFaceLoader
from .processor import IngestionProcessor

__all__ = ["DatasetRegistry", "HuggingFaceLoader", "IngestionProcessor"]
