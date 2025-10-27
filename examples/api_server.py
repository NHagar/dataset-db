"""
Example: API Server

Demonstrates running the FastAPI server for domain and URL queries.

Prerequisites:
- Run parquet_ingestion.py first to create Parquet files
- Run index_building.py to build indexes

Then start the server and query it:
```bash
# Start the server
uv run python examples/api_server.py

# In another terminal, query the API:
curl http://localhost:8000/v1/domain/example.com
curl "http://localhost:8000/v1/domain/example.com/datasets/1/urls?limit=10"
```
"""

import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def main():
    """Run the API server."""
    import uvicorn

    from dataset_db.api.server import app

    # Check if data exists
    base_path = Path("./data")
    manifest_path = base_path / "index" / "manifest.json"

    if not manifest_path.exists():
        print("ERROR: No indexes found!")
        print("Please run the following first:")
        print("  1. uv run python examples/parquet_ingestion.py")
        print("  2. uv run python examples/index_building.py")
        return

    print("=" * 80)
    print("Starting Dataset DB API Server")
    print("=" * 80)
    print()
    print("The server will start on http://0.0.0.0:8000")
    print()
    print("API Endpoints:")
    print("  GET /                                              - Health check")
    print("  GET /v1/domain/{domain}                            - List datasets for domain")
    print("  GET /v1/domain/{domain}/datasets/{id}/urls         - Get URLs (paginated)")
    print()
    print("Example queries:")
    print('  curl http://localhost:8000/')
    print('  curl http://localhost:8000/v1/domain/example.com')
    print('  curl "http://localhost:8000/v1/domain/example.com/datasets/1/urls?limit=5"')
    print()
    print("=" * 80)
    print()

    # Run server
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


if __name__ == "__main__":
    main()
