"""
URL and domain normalization utilities.

Handles canonicalization, eTLD+1 extraction, and ID generation.
"""

from .url_normalizer import URLNormalizer, NormalizedURL

__all__ = ["URLNormalizer", "NormalizedURL"]
