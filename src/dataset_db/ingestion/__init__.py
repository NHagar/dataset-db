"""
Data ingestion pipeline.

Handles loading from HuggingFace datasets and processing into normalized format.
"""

from .duplicate_tracker import DuplicateTracker
from .hf_loader import HuggingFaceLoader
from .processor import IngestionProcessor

__all__ = ["DuplicateTracker", "HuggingFaceLoader", "IngestionProcessor"]
