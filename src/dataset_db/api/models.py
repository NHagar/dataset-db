"""
API response models.

Defines Pydantic models for API responses per spec.md §4.2.
"""

from pydantic import BaseModel, Field


class DatasetInfo(BaseModel):
    """Information about a dataset containing a domain."""

    dataset_id: int = Field(..., description="Dataset ID")
    url_count_est: int | None = Field(
        None, description="Estimated URL count for this domain in this dataset"
    )


class DomainResponse(BaseModel):
    """Response for GET /v1/domain/{domain}."""

    domain: str = Field(..., description="Normalized domain string")
    domain_id: int = Field(..., description="Internal domain ID")
    datasets: list[DatasetInfo] = Field(
        default_factory=list, description="Datasets containing this domain"
    )


class URLItem(BaseModel):
    """A single URL record."""

    url_id: int = Field(..., description="URL ID (xxh3_64 hash)")
    url: str = Field(..., description="Full reconstructed URL")
    ts: str | None = Field(None, description="Timestamp (if available)")


class URLsResponse(BaseModel):
    """Response for GET /v1/domain/{domain}/datasets/{dataset_id}/urls."""

    domain: str = Field(..., description="Normalized domain string")
    dataset_id: int = Field(..., description="Dataset ID")
    total_est: int | None = Field(
        None, description="Estimated total URLs for this (domain, dataset) pair"
    )
    items: list[URLItem] = Field(
        default_factory=list, description="URL records for current page"
    )
    next_offset: int | None = Field(
        None, description="Offset for next page (null if no more results)"
    )
