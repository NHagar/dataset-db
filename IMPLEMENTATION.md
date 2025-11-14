# Implementation Log

**Project:** dataset-db - Domain → Datasets → URLs at extreme scale
**Status:** Milestone 5 Complete
**Last Updated:** 2025-10-27

---

## Current State

✅ **Milestone 1:** URL Normalization & Ingestion
✅ **Milestone 2:** Parquet Storage with Partitioning
✅ **Milestone 3:** Index Building (Domain Dict, MPHF, Membership, Postings)
✅ **Milestone 4:** API Layer (FastAPI Server)
✅ **Milestone 5:** Incremental Updates
⏭️ **Milestone 6:** Pre-Aggregations & Optimizations (Future)

**Test Suite:** 149 unit tests
**Code Quality:** All linting checks pass (`ruff check`)

---

## Notes (2025-11-10)

- Fixed the regression where URL lookups returned empty results despite valid memberships. The parquet files store the canonical domain string (not the MPHF-assigned ID), so `QueryService` now filters row groups by the `domain` column instead of the synthetic `domain_id`. Verified via a loader/query smoke test that the first sample domain now returns real URLs.
- Added a focused unit test (`TestQueryService::test_get_urls_returns_items_for_matching_domain`) to ensure we never regress on URL retrieval for valid domain/dataset pairs.

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
- ✅ **P0:** Domain ID stability in incremental builds (fixed 2025-10-27)
  - Issue: `merge_sorted_domains()` was re-sorting all domains, causing existing domain IDs to shift when new domains sorted before existing ones
  - Impact: Loaded membership/postings indexes used old domain IDs, causing incorrect lookups after incremental build
  - Fix: Append new domains to end of list instead of merge-sorting, preserving all existing domain IDs
  - Test: [test_domain_id_stability()](tests/unit/test_incremental.py:224) verifies domain IDs remain stable across builds
- ✅ **P0:** FileRegistry API usage in URL query path (fixed 2025-10-27)
  - Issue: `QueryService.get_urls_for_domain_dataset()` called non-existent `get_file()` method and dereferenced `.path` attribute
  - Impact: All `/v1/domain/{domain}/datasets/{dataset_id}/urls` requests would return 500 error
  - Fix: Use correct `get_file_info(file_id)` method and access `["parquet_rel_path"]` dict key
- ✅ **P1:** Test client lifespan patching ineffective (fixed 2025-10-27)
  - Issue: API tests tried to monkey-patch lifespan after app creation, causing tests to load from wrong directory
  - Impact: Tests would fail if `./data` directory was absent; couldn't exercise endpoints with temporary test data
  - Fix: Initialize loader before TestClient creation and copy routes to new app instance with test lifespan
- ✅ **P1:** PostingsIndex.lookup() API mismatch (fixed 2025-10-27)
  - Issue: Query service called `lookup(domain_id, dataset_id)` but method requires `lookup(version, domain_id, dataset_id)`
  - Impact: URL queries would fail with TypeError
  - Fix: Pass version from loader's `_current_version.version` as first argument
- ✅ **P0:** Postings shard path resolution in loader (fixed 2025-10-27)
  - Issue: `IndexLoader` instantiated `PostingsIndex` with `base_path/index/{version}`, duplicating the version when `lookup()`
    constructed shard paths
  - Impact: All postings lookups read from non-existent directories, yielding zero results for every domain/dataset
  - Fix: Initialize `PostingsIndex` with the storage root (`base_path`) so shard paths resolve to `index/{version}/postings/...`
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

## End-to-End Testing Tools

**New Scripts Added (2025-10-28):**

1. **[examples/e2e_test.py](examples/e2e_test.py)** - Comprehensive end-to-end testing
   - Supports HuggingFace and local file ingestion
   - Runs full pipeline: ingest → build → query → validate
   - Interactive API server launcher
   - Validation checks for correctness

2. **[examples/inspect_data.py](examples/inspect_data.py)** - Data inspection utility
   - Shows storage and index statistics
   - Displays sample data and domains
   - Query specific domains
   - Reports dataset registry

3. **[E2E_TESTING.md](E2E_TESTING.md)** - Complete testing guide
   - Step-by-step instructions
   - Command-line options reference
   - Troubleshooting tips
   - Performance guidance

4. **[QUICKSTART.md](QUICKSTART.md)** - Quick reference guide
   - 5-minute getting started
   - Common commands
   - Example workflows

**Usage:**

```bash
# Test with your HuggingFace dataset
python examples/e2e_test.py --dataset reddit --source huggingface --username nhagar

# Test with local file
python examples/e2e_test.py --dataset my_data --source local --file urls.csv

# Inspect existing data
python examples/inspect_data.py

# Query specific domain
python examples/inspect_data.py --domain example.com
```

---

**Last Build:** 164,099 domains indexed in 2.2 seconds

### 2024-11-20
- Fixed MPHF loader to reset `domain_to_id` and log the stored domain count from the file header. Prevents misleading "0 domains" messages when loading persisted MPHFs.

### 2025-11-14
- Implemented a persistent dataset registry stored at `data/registry/dataset_registry.json` so dataset IDs survive multiple ingestion sessions.
- `IngestionProcessor` now depends on the registry (instead of the transient ID generator) and `examples/inspect_data.py` can display the mapping.
- Added regression tests covering registry persistence and sequential assignment to avoid future regressions.

---

## Milestone 5: Incremental Updates (In Progress)

### Overview

Implementing incremental index updates to avoid full rebuilds when adding new datasets or updating existing ones. This aligns with spec.md §6 requirements for efficient incremental processing.

### Design Strategy

**Key Principle:** Load previous version, merge new data, write new version atomically.

#### 1. Domain Dictionary Incremental Updates

**Challenge:** Domain dictionary is sorted, so inserting new domains requires merging.

**Approach:**
- Load previous `domains.txt.zst` (streaming decompression)
- Extract new domains from new Parquet files only
- Merge-sort: old domains + new domains → new sorted list
- Write new `domains.txt.zst`
- Domain IDs may change when new domains inserted mid-sequence

**Implementation:**
```python
def build_incremental(version, prev_version):
    # Load old domains
    old_domains = load_domains(prev_version)

    # Extract new domains from new files only
    new_domains = extract_from_new_files(...)

    # Merge sort
    merged = merge_sorted(old_domains, new_domains)

    # Write new dictionary
    write_domains(version, merged)
```

**Implementation Detail - Domain ID Stability:**
To preserve correctness of incremental builds, new domains are **appended** to the end of the existing domain list rather than merge-sorted. This ensures:
- Existing domain IDs remain unchanged across versions
- Old membership/postings data remains valid
- Only new domains get new IDs (at the end)

This is critical because loaded indexes from the previous version use the old domain IDs. If we re-sorted and changed IDs, lookups would return incorrect results.

#### 2. MPHF Incremental Updates

**Challenge:** Simple hash-based MPHF doesn't support true incremental updates.

**Options:**
1. **Full Rebuild (Current Approach):** Fast enough for millions of domains (~40ms for 164K)
2. **Two-Level Scheme:** Stable base MPHF + overflow map for new domains
3. **BBHash:** Supports incremental builds natively

**Chosen: Full Rebuild for Now**
- 164K domains → 40ms rebuild
- Even 10M domains → ~2.5s rebuild (acceptable)
- Simplest implementation
- Can switch to two-level scheme if rebuild becomes bottleneck

#### 3. Membership Index Incremental Updates

**Challenge:** Efficiently merge new (domain, dataset) relationships with existing bitmaps.

**Approach:**
- Load previous `domain_to_datasets.roar` (memory-mapped or streamed)
- For each new Parquet file, extract (domain, dataset) pairs
- For each pair:
  - Lookup domain_id from new MPHF
  - Load existing bitmap for domain_id (if exists)
  - Add dataset_id to bitmap: `bitmap.add(dataset_id)`
- Write merged bitmaps to new version

**Implementation:**
```python
def build_membership_incremental(version, prev_version, new_files):
    # Load previous index
    prev_index = MembershipIndex()
    prev_index.load(f"{prev_version}/domain_to_datasets.roar")

    # Copy existing bitmaps (shallow copy, cheap)
    new_bitmaps = {did: BitMap(bm) for did, bm in prev_index.domain_bitmaps.items()}

    # Process new files only
    for file in new_files:
        domains = extract_domains(file)
        dataset_id = parse_dataset_id(file)

        for domain in domains:
            domain_id = mphf.lookup(domain)
            if domain_id not in new_bitmaps:
                new_bitmaps[domain_id] = BitMap()
            new_bitmaps[domain_id].add(dataset_id)

    # Save merged index
    save(version, new_bitmaps)
```

**Optimization:** Roaring bitmaps support efficient `union()` operation if batching updates.

#### 4. Postings Index Incremental Updates

**Challenge:** Append new (domain, dataset) → row-group mappings without reprocessing all files.

**Approach:**
- Load previous postings index (streaming per shard)
- Process new Parquet files only
- For each new file/row-group:
  - Extract unique domains
  - Append (file_id, row_group) to postings for each (domain_id, dataset_id) pair
- Merge old + new postings per shard
- Write merged postings

**Implementation:**
```python
def build_postings_incremental(version, prev_version, new_files, file_registry):
    # Load previous postings
    prev_postings = PostingsIndex.load(prev_version)

    # Initialize with previous data
    new_postings = {key: list(val) for key, val in prev_postings.postings.items()}

    # Process new files only
    for file in new_files:
        file_id = file_registry.get_file_id(file)
        dataset_id = parse_dataset_id(file)

        # Get row groups
        metadata = pq.read_metadata(file)
        for rg_idx in range(metadata.num_row_groups):
            domains = extract_domains_from_row_group(file, rg_idx)

            for domain in domains:
                domain_id = mphf.lookup(domain)
                key = (domain_id, dataset_id)

                if key not in new_postings:
                    new_postings[key] = []

                new_postings[key].append((file_id, rg_idx))

    # Save merged postings
    save_postings(version, new_postings)
```

**Compaction Strategy:**
- Postings lists grow with appends
- Periodically compact: sort and deduplicate (file_id, row_group) tuples
- Could track "compaction needed" flag when list > threshold (e.g., 100 entries)

#### 5. File Registry Incremental Updates

**Challenge:** Assign file IDs to new files without reassigning existing IDs.

**Approach:**
- Load previous `files.tsv.zst`
- Scan for new Parquet files not in registry
- Assign sequential file IDs starting from `max(existing_ids) + 1`
- Write merged registry (old + new)

**Implementation:**
```python
def build_file_registry_incremental(version, prev_version):
    # Load previous registry
    prev_files = load_registry(prev_version)
    next_file_id = max(f["file_id"] for f in prev_files) + 1 if prev_files else 0

    # Find new files
    all_files = find_all_parquet_files()
    existing_paths = {f["parquet_rel_path"] for f in prev_files}
    new_files = [f for f in all_files if f not in existing_paths]

    # Assign IDs to new files
    new_entries = []
    for file_path in new_files:
        entry = {
            "file_id": next_file_id,
            "dataset_id": parse_dataset_id(file_path),
            "domain_prefix": parse_domain_prefix(file_path),
            "parquet_rel_path": file_path,
        }
        new_entries.append(entry)
        next_file_id += 1

    # Write merged registry
    write_registry(version, prev_files + new_entries)
```

#### 6. Version Management

**Workflow:**
1. Caller invokes `builder.build_incremental(new_dataset_ids)`
2. Builder determines which Parquet files are new (not in previous file registry)
3. Builder loads previous version's indexes
4. Builder processes only new files, merging with previous data
5. Builder writes new version atomically
6. Manifest updated to point to new version

**Manifest Changes:**
- No changes needed! Existing `Manifest` already supports multiple versions
- Each incremental build creates a new version directory

### Implementation Plan

**Phase 1: Core Incremental Logic**
1. Add `load()` methods to all index components to load previous version
2. Modify `build()` methods to accept previous data and merge
3. Add file tracking to determine "new files since previous version"

**Phase 2: IndexBuilder Integration**
1. Implement `IndexBuilder.build_incremental()` orchestration
2. Load previous version from manifest
3. Determine new files
4. Call incremental build methods for each component

**Phase 3: Testing & Validation**
1. Unit tests for merge logic in each component
2. Integration test: build v1, add data, build v2 incrementally
3. Verify correctness: query results identical to full rebuild

**Phase 4: Compaction (Future)**
1. Add compaction logic for postings lists
2. Trigger compaction when list length > threshold
3. Background compaction job option

### Performance Expectations

**Current Full Build (164K domains, 257 files):** ~2.2s

**Expected Incremental Build (adding 10% more data):**
- Domain dictionary merge: ~50ms (stream old + new)
- MPHF rebuild: ~45ms (180K domains)
- File registry update: ~5ms (append 26 files)
- Membership merge: ~30ms (load + update 16.4K bitmaps)
- Postings merge: ~200ms (append to existing postings)
- **Total: ~330ms (6.7x faster than full rebuild)**

**Scaling:**
- At 10M domains, full rebuild ≈ 30s
- Incremental with 10% new data ≈ 5s (6x faster)

### Trade-offs & Future Optimizations

**Current Approach:**
- ✅ Simple implementation
- ✅ No data structure changes needed
- ✅ Atomic versioning preserved
- ⚠️ Domain IDs may shift (acceptable, handled by MPHF)
- ⚠️ MPHF full rebuild (acceptable for now)

**Future Optimizations:**
1. **Two-Level MPHF:** Stable base + overflow map (avoid full MPHF rebuild)
2. **Incremental Domain Dict:** Track domain ID remapping and update postings/membership accordingly
3. **Lazy Compaction:** Mark fragmented postings, compact in background
4. **Parallel Processing:** Process shards in parallel during merge
5. **Memory-Mapped Loads:** Load previous indexes via mmap for zero-copy

### Implementation Complete

**Status:** ✅ All components implemented and tested

**Components Implemented:**
1. [FileRegistry.build_incremental()](src/dataset_db/index/file_registry.py:220) - Incremental file registry with new file detection
2. [DomainDictionary.build_incremental()](src/dataset_db/index/domain_dict.py:274) - Domain merge-sort with deduplication
3. [MembershipIndex.build_incremental()](src/dataset_db/index/membership.py:404) - Roaring bitmap merging
4. [PostingsIndex.build_incremental()](src/dataset_db/index/postings.py:557) - Postings append with lazy loading
5. [IndexBuilder.build_incremental()](src/dataset_db/index/builder.py:121) - Orchestration layer

**Testing:**
- 6 unit tests in [test_incremental.py](tests/unit/test_incremental.py)
  - Including critical domain ID stability test
- All 149 tests passing
- Example script: [examples/incremental_updates.py](examples/incremental_updates.py)

**Key Features:**
- **Auto-detection:** Automatically identifies new Parquet files since previous version
- **Selective processing:** Only scans new files, not entire dataset
- **Atomic versioning:** Each incremental build creates a new version
- **Fallback:** Gracefully falls back to full build if no previous version exists
- **Idempotent:** Running incremental with no new files returns previous version

**Performance Results (Example Run):**
```
Initial build:    1 file,  1 domain  (~50ms)
Incremental #1:  +3 files, +3 domains (~120ms) - 2.4x faster than full rebuild
Incremental #2:  +2 files, +1 domain  (~80ms)  - 3.1x faster than full rebuild
```

**Usage:**
```python
from dataset_db.index import IndexBuilder

# After ingesting new data
builder = IndexBuilder(base_path)
version = builder.build_incremental()

# Automatically:
# 1. Loads previous version from manifest
# 2. Identifies new Parquet files
# 3. Merges with existing indexes
# 4. Publishes new version atomically
```

---

**Last Build:** 164,099 domains indexed in 2.2 seconds
