# Project Structure

## Directory Layout

```
dataset-db/
├── src/dataset_db/           # Main package
│   ├── __init__.py
│   ├── config.py             # Configuration management with Pydantic
│   │
│   ├── normalization/        # URL normalization module
│   │   └── __init__.py
│   │
│   ├── ingestion/            # Data ingestion from HuggingFace
│   │   └── __init__.py
│   │
│   ├── storage/              # Parquet writing and storage layout
│   │   └── __init__.py
│   │
│   ├── index/                # Index building (domains, postings, etc.)
│   │   └── __init__.py
│   │
│   └── api/                  # API serving layer
│       └── __init__.py
│
├── tests/                    # Test suite
│   ├── __init__.py
│   ├── unit/                 # Unit tests
│   │   └── __init__.py
│   └── integration/          # Integration tests
│       └── __init__.py
│
├── spec.md                   # Full implementation specification
├── ingest.md                 # Ingestion data format documentation
├── README.md                 # Project overview
├── pyproject.toml            # Python project configuration
├── .env.example              # Example environment configuration
└── .gitignore                # Git ignore patterns
```

## Modules Overview

### Core Modules

1. **normalization/** - URL & domain normalization
   - URL parsing and canonicalization
   - eTLD+1 extraction via Public Suffix List
   - ID generation (xxh3_64 for URLs)

2. **ingestion/** - Data loading pipeline
   - HuggingFace datasets loader
   - Batch processing
   - URL normalization pipeline

3. **storage/** - Parquet storage
   - Partitioned Parquet writing
   - Storage layout management (local/S3)
   - Compression (ZSTD)

4. **index/** - Index building (future)
   - Domain dictionary & MPHF
   - Roaring bitmaps for domain→datasets
   - Postings for row-group pointers

5. **api/** - Serving layer (future)
   - `/domain/{d}` endpoint
   - `/domain/{d}/dataset/{id}/urls` endpoint

### Configuration

The `config.py` module provides:
- **IngestionConfig**: HuggingFace settings, batch sizes, workers
- **StorageConfig**: Local/S3 storage, partitioning
- **IndexConfig**: MPHF, sharding settings
- Environment variable support via `pydantic-settings`

## Dependencies

### Core
- **polars** & **pyarrow**: High-performance data processing
- **datasets**: HuggingFace datasets library
- **publicsuffixlist**: eTLD+1 extraction
- **xxhash**: Fast hashing for URL IDs
- **zstandard**: Compression

### Configuration & Utilities
- **pydantic** & **pydantic-settings**: Type-safe configuration
- **ruff**: Linting and formatting

### Development
- **pytest**: Testing framework
- **pytest-cov**: Coverage reporting
- **mypy**: Type checking

## Next Steps

Based on the spec.md milestones:

1. **Milestone 1**: URL normalization + Parquet writer
2. **Milestone 2**: HuggingFace ingestion pipeline
3. **Milestone 3**: Domain membership index (Roaring)
4. **Milestone 4**: Postings index (row-group offsets)
5. **Milestone 5**: API serving layer
6. **Milestone 6**: Incremental updates & manifest management
