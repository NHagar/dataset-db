# Implementation Log

**Project:** dataset-db - Domain → Datasets → URLs at extreme scale
**Status:** Milestone 4 Complete
**Last Updated:** 2025-10-27

---

## Current State

✅ **Milestone 1:** URL Normalization & Ingestion
✅ **Milestone 2:** Parquet Storage with Partitioning
✅ **Milestone 3:** Index Building (Domain Dict, MPHF, Membership, Postings)
✅ **Milestone 4:** API Layer (FastAPI Server)
⏭️ **Milestone 5:** Incremental Updates (Next)

**Test Suite:** 145+ unit tests
**Code Quality:** All linting checks pass (`ruff check`)

---

## Milestone 1: URL Normalization & Ingestion ✅

### Components
- **URL Normalizer** ([url_normalizer.py](src/dataset_db/normalization/url_normalizer.py)) - Full URL canonicalization per spec.md §1.1
- **ID Generator** ([ids.py](src/dataset_db/normalization/ids.py)) - xxh3_64 hashing for URL/domain IDs
- **Ingestion Processor** ([processor.py](src/dataset_db/ingestion/processor.py)) - Batch processing pipeline
- **HuggingFace Loader** ([hf_loader.py](src/dataset_db/ingestion/hf_loader.py)) - Dataset streaming

### Key Features
- eTLD+1 domain extraction via Public Suffix List
- Punycode conversion for internationalized domains
- Query parameter sorting with duplicate key preservation
- Configurable batch sizes (default: 1M rows)
- Dataset ID registry for consistent mapping

### Performance
- ~100k URLs/second normalization on single core
- Streaming support for datasets larger than memory

### Tests
- 45 unit tests covering normalization, ID generation, ingestion

---

## Milestone 2: Parquet Storage ✅

### Components
- **Storage Layout** ([layout.py](src/dataset_db/storage/layout.py)) - Hive-style partitioning
- **Parquet Writer** ([parquet_writer.py](src/dataset_db/storage/parquet_writer.py)) - Optimized writing with buffering

### Storage Layout
```
data/urls/
  dataset_id={id}/
    domain_prefix={hh}/    # First 2 hex chars of domain hash
      part-*.parquet       # 128MB row groups
```

### Key Features
- **Partitioning:** By dataset_id and domain_prefix (2 hex chars)
- **Compression:** ZSTD level 6, dictionary encoding for strings
- **Buffering:** 128MB per-partition buffers before flushing
- **Schema:** domain_id, url_id, scheme, host, path_query, domain

### Performance
- 10-20x compression ratio for typical URL datasets
- 128MB row groups enable efficient Parquet statistics
- Partition buffering reduces file count by 3,000x

### Design Decisions
**Why remove raw_url?**
- URLs reconstructable: `f"{scheme}://{host}{path_query}"`
- 3x disk space savings (1.8 TB → 600 GB for 10B URLs)
- Normalized form is source of truth

### Tests
- 50 unit tests covering layout, writing, reading, compression

---

## Milestone 3: Index Building ✅

### Components

| Component | File | Purpose |
|-----------|------|---------|
| Domain Dictionary | [domain_dict.py](src/dataset_db/index/domain_dict.py) | Sorted unique domains (compressed) |
| MPHF | [mphf.py](src/dataset_db/index/mphf.py) | Domain string → ID lookup |
| Membership Index | [membership.py](src/dataset_db/index/membership.py) | Domain → Datasets (Roaring bitmaps) |
| File Registry | [file_registry.py](src/dataset_db/index/file_registry.py) | File ID → Parquet path mapping |
| Postings Index | [postings.py](src/dataset_db/index/postings.py) | (Domain, Dataset) → Row groups |
| Manifest | [manifest.py](src/dataset_db/index/manifest.py) | Version management |
| Builder | [builder.py](src/dataset_db/index/builder.py) | Orchestrates all builds |

### Index Structure
```
data/index/
├── manifest.json                    # Current version pointer
└── {version}/                       # e.g., 2025-10-24T22:28:03Z
    ├── domains.txt.zst             # Sorted domains (2.8x compressed)
    ├── domains.mphf                # Hash lookup structure
    ├── domain_to_datasets.roar     # Roaring bitmaps
    ├── files.tsv.zst               # File registry (13.3x compressed)
    └── postings/{shard}/           # Sharded postings (default: 1024 shards)
        ├── postings.idx.zst        # Index entries
        └── postings.dat.zst        # Varint-encoded payloads
```

### Performance (164K domains, 257 files, ~10M rows)

| Stage | Time | Size |
|-------|------|------|
| Domain dictionary | 400ms | 984 KB |
| MPHF | 40ms | 1.8 MB |
| File registry | 10ms | 1.1 KB |
| Membership index | 250ms | 4.9 MB |
| Postings index | 1.5s | ~500 KB (16 shards) |
| **Total** | **~2.2s** | **~7.5 MB** |

### Key Design Decisions

**1. Simple Hash-Based MPHF vs BBHash**
- **Chosen:** xxh3_64 with collision handling
- **Rationale:** Zero collisions observed (164K domains), no C++ dependencies
- **Trade-off:** 11 bytes/domain vs 2-3 bits/domain with BBHash
- **Scalability:** For 1B domains: ~11 GB vs ~375 MB (still manageable)

**2. Varint Encoding for Postings**
- Most file_ids and row_group numbers < 128 → 1 byte
- ~80% space savings vs fixed-width encoding

**3. Sharded Postings Index**
- 1024 shards keeps each shard < 10MB
- Enables parallel processing and lazy loading
- Binary search faster in smaller shards

**4. Roaring Bitmaps for Membership**
- Industry standard, excellent compression
- Fast set operations (union, intersection)
- pyroaring provides mature Python bindings

### Tests
- 32 unit tests covering all index components
- Integration tested via example script

---

## Implementation Details

### Dependencies

```toml
# Core
polars>=1.0.0          # DataFrames
pyarrow>=18.0.0        # Parquet support
datasets>=3.0.0        # HuggingFace datasets
xxhash>=3.0.0          # Fast hashing
zstandard>=0.23.0      # Compression
pydantic>=2.0.0        # Configuration
pyroaring>=1.0.3       # Roaring bitmaps
publicsuffixlist>=1.0.0  # eTLD+1 extraction
```

### Configuration

Environment variables (via `.env`):
```bash
# Ingestion
INGEST__BATCH_SIZE=1000000              # 1M rows per batch
INGEST__PARTITION_BUFFER_SIZE=134217728 # 128MB buffer
INGEST__COMPRESSION_LEVEL=6             # ZSTD level

# Storage
STORAGE__BASE_PATH=./data
STORAGE__DOMAIN_PREFIX_CHARS=2

# Indexing
INDEX__POSTINGS_SHARDS=1024
INDEX__MPHF_GAMMA=2.0
```

### Quick Start

```python
from pathlib import Path
from dataset_db.ingestion import IngestionProcessor
from dataset_db.storage import ParquetWriter
from dataset_db.index import IndexBuilder

# 1. Ingest URLs
processor = IngestionProcessor()
writer = ParquetWriter(base_path="./data")

# Process batch
import polars as pl
df = pl.DataFrame({"url": ["https://example.com/path"]})
normalized = processor.process_batch(df, "my_dataset")
writer.write_batch(normalized)
writer.flush()

# 2. Build indexes
builder = IndexBuilder(base_path=Path("./data"))
version = builder.build_all()

# 3. Query
from dataset_db.index import Manifest, SimpleMPHF, MembershipIndex

manifest = Manifest(Path("./data"))
manifest.load()
current = manifest.get_current_version()

# Load MPHF and lookup domain
mphf = SimpleMPHF()
mphf.load(Path(f"./data/{current.domains_mphf}"))
domain_id = mphf.lookup("example.com")
```

See [examples/](examples/) for complete demonstrations.

---

## Milestone 4: API Layer ✅

### Components

| Component | File | Purpose |
|-----------|------|---------|
| Response Models | [models.py](src/dataset_db/api/models.py) | Pydantic models for API responses |
| Index Loader | [loader.py](src/dataset_db/api/loader.py) | Lazy loading + LRU caching |
| Query Service | [query.py](src/dataset_db/api/query.py) | Domain/URL query algorithms |
| FastAPI Server | [server.py](src/dataset_db/api/server.py) | HTTP endpoints |

### API Endpoints

#### GET /v1/domain/{domain}
Returns datasets containing the given domain.

**Example:**
```bash
curl http://localhost:8000/v1/domain/example.com
```

**Response:**
```json
{
  "domain": "example.com",
  "domain_id": 12345,
  "datasets": [
    {"dataset_id": 1, "url_count_est": null},
    {"dataset_id": 2, "url_count_est": null}
  ]
}
```

#### GET /v1/domain/{domain}/datasets/{dataset_id}/urls
Returns paginated URLs for a (domain, dataset) pair.

**Parameters:**
- `offset` (int, default: 0) - Pagination offset
- `limit` (int, default: 1000, max: 10000) - Number of URLs to return

**Example:**
```bash
curl "http://localhost:8000/v1/domain/example.com/datasets/1/urls?offset=0&limit=10"
```

**Response:**
```json
{
  "domain": "example.com",
  "dataset_id": 1,
  "total_est": null,
  "items": [
    {
      "url_id": 12345678901234,
      "url": "https://example.com/path?query=value",
      "ts": null
    }
  ],
  "next_offset": 10
}
```

### Key Features

**Index Loader ([loader.py](src/dataset_db/api/loader.py:23))**
- Singleton pattern for global index access
- Lazy loading of all index structures at startup
- LRU caching for hot domains (1000 entries)
- Memory-efficient domain dictionary loading
- Cached methods:
  - `lookup_domain_id(domain)` - Domain → ID lookup
  - `get_domain_string(domain_id)` - ID → Domain reverse lookup
  - `get_datasets_for_domain(domain_id)` - Domain → Datasets

**Query Service ([query.py](src/dataset_db/api/query.py:17))**
- Implements spec.md §4.3 query algorithms
- Domain lookup via MPHF (O(1) average)
- Dataset membership via Roaring bitmaps
- URL retrieval with row-group-level Parquet reads
- Pagination support with offset/limit

**FastAPI Server ([server.py](src/dataset_db/api/server.py))**
- Async request handlers
- Automatic index loading at startup (lifespan context)
- Input validation via Pydantic
- Error handling with appropriate HTTP status codes
- OpenAPI documentation at `/docs`

### Performance

- **Domain lookup:** O(1) via MPHF + LRU cache
- **Dataset membership:** O(1) via Roaring bitmap lookup
- **URL retrieval:** Efficient Parquet row-group scans with filtering
- **Memory usage:** ~10-20MB for indexes (164K domains)

### Running the Server

```bash
# Start server
uv run python examples/api_server.py

# Or directly with uvicorn
uv run uvicorn dataset_db.api.server:app --host 0.0.0.0 --port 8000
```

Visit `http://localhost:8000/docs` for interactive API documentation.

### Tests

- 18 unit tests covering all API components
- Test fixture creates temporary test data + indexes
- Tests loader initialization, caching, query service, endpoints
- See [test_api.py](tests/unit/test_api.py)

### Current Limitations

- URL counts (`total_est`, `url_count_est`) not yet computed (Milestone 6)
- Row-group reading via Polars scans full file (production should use PyArrow/DuckDB for true row-group reads)
- No streaming response for very large result sets

### Future Optimizations

- Pre-computed (domain, dataset) → url_count table
- True row-group-level reads via PyArrow
- Streaming JSON responses for large result sets
- Response compression (gzip/brotli)
- Rate limiting and API keys

---

## Known Issues & Notes

### Fixed Issues
- ✅ **P1:** File registry CSV serialization bug (fixed 2025-10-24)
  - Issue: `df.write_csv()` returns None, causing AttributeError
  - Fix: Use StringIO buffer to capture CSV output

### Current Limitations
- Incremental builds not yet implemented (Milestone 5)
- S3 backend not yet supported (Milestone 6)
- No pre-aggregated counts yet (Milestone 6)

### Future Optimizations
- BBHash for MPHF at billion-domain scale
- Pre-computed (domain, dataset) → count table
- Memory-mapped index files for zero-copy reads
- Async Parquet reads with prefetching

---

## File Structure

```
src/dataset_db/
├── normalization/
│   ├── url_normalizer.py    # URL canonicalization
│   └── ids.py               # ID generation (xxh3_64)
├── ingestion/
│   ├── hf_loader.py         # HuggingFace dataset loader
│   └── processor.py         # Batch processing
├── storage/
│   ├── layout.py            # Hive-style partitioning
│   └── parquet_writer.py    # Optimized Parquet writer
├── index/
│   ├── domain_dict.py       # Domain dictionary builder
│   ├── mphf.py              # Minimal perfect hash
│   ├── membership.py        # Roaring bitmaps (domain→datasets)
│   ├── file_registry.py     # File ID registry
│   ├── postings.py          # Postings index
│   ├── manifest.py          # Version management
│   └── builder.py           # Index builder orchestrator
├── api/
│   ├── models.py            # Pydantic response models
│   ├── loader.py            # Index loader with caching
│   ├── query.py             # Query service
│   └── server.py            # FastAPI server
└── config.py                # Configuration management

tests/unit/                  # 145+ tests
examples/                    # Working examples + API server
```

---

## References

- **Specification:** [spec.md](spec.md) - Complete system design
- **Examples:** [examples/](examples/) - Working code demonstrations
- **Tests:** [tests/unit/](tests/unit/) - Test coverage

---

**Last Build:** 164,099 domains indexed in 2.2 seconds
