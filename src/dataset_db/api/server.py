"""
FastAPI server for domain and URL queries.

Implements API endpoints per spec.md §4.2.
"""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from dataset_db.api.loader import get_loader, init_loader
from dataset_db.api.models import DomainResponse, URLsResponse
from dataset_db.api.query import QueryService

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for the FastAPI app.

    Loads indexes on startup, cleans up on shutdown.
    """
    # Startup: load indexes
    logger.info("Starting up: loading indexes...")
    base_path = Path("./data")  # TODO: Make configurable via env var
    init_loader(base_path)
    logger.info("Indexes loaded successfully")

    yield

    # Shutdown
    logger.info("Shutting down...")


# Create FastAPI app
app = FastAPI(
    title="Dataset DB API",
    description="Domain → Datasets → URLs query API",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "ok", "message": "Dataset DB API is running"}


@app.get("/v1/domain/{domain}", response_model=DomainResponse)
async def get_domain_datasets(domain: str) -> DomainResponse:
    """
    Get list of datasets containing the given domain.

    Args:
        domain: Domain string (e.g., "example.com")

    Returns:
        DomainResponse with datasets and counts

    Raises:
        404: If domain not found
    """
    try:
        loader = get_loader()
        service = QueryService(loader)
        return service.get_datasets_for_domain(domain)
    except ValueError as e:
        logger.warning(f"Domain lookup failed: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error in get_domain_datasets: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/v1/domain/{domain}/datasets/{dataset_id}/urls", response_model=URLsResponse)
async def get_domain_dataset_urls(
    domain: str,
    dataset_id: int,
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of URLs to return"),
) -> URLsResponse:
    """
    Get URLs for a given (domain, dataset) pair with pagination.

    Args:
        domain: Domain string (e.g., "example.com")
        dataset_id: Dataset ID
        offset: Offset for pagination (default: 0)
        limit: Maximum number of URLs to return (default: 1000, max: 10000)

    Returns:
        URLsResponse with paginated URLs

    Raises:
        404: If domain not found or dataset doesn't contain domain
    """
    try:
        loader = get_loader()
        service = QueryService(loader)
        return service.get_urls_for_domain_dataset(domain, dataset_id, offset, limit)
    except ValueError as e:
        logger.warning(f"URL lookup failed: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error in get_domain_dataset_urls: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.exception_handler(404)
async def not_found_handler(request, exc):
    """Custom 404 handler."""
    return JSONResponse(status_code=404, content={"detail": str(exc.detail)})


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    """Custom 500 handler."""
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


def main():
    """Run the server (for development)."""
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


if __name__ == "__main__":
    main()
