"""
Data ingestion pipeline.

Handles loading from HuggingFace datasets and processing into normalized format.
"""

from .hf_loader import HuggingFaceLoader
from .processor import IngestionProcessor

__all__ = ["HuggingFaceLoader", "IngestionProcessor"]
