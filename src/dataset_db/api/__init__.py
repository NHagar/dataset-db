"""
API serving layer.

Handles domain and URL queries.
"""

from dataset_db.api.loader import IndexLoader, get_loader, init_loader
from dataset_db.api.models import DatasetInfo, DomainResponse, URLItem, URLsResponse
from dataset_db.api.query import QueryService
from dataset_db.api.server import app

__all__ = [
    "IndexLoader",
    "get_loader",
    "init_loader",
    "DatasetInfo",
    "DomainResponse",
    "URLItem",
    "URLsResponse",
    "QueryService",
    "app",
]
