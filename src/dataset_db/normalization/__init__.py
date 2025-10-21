"""
URL and domain normalization utilities.

Handles canonicalization, eTLD+1 extraction, and ID generation.
"""

from .ids import IDGenerator, get_id_generator, reset_id_generator
from .url_normalizer import NormalizedURL, URLNormalizer

__all__ = [
    "URLNormalizer",
    "NormalizedURL",
    "IDGenerator",
    "get_id_generator",
    "reset_id_generator",
]
