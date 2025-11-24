# Implementation Log (Compact)

**Project:** dataset-db — Domain → Datasets → URLs at scale  
**Status:** Milestone 5 complete (Incremental Updates)  
**Tests:** 149 unit tests; `uv run ruff check . --fix` clean  
**Last Updated:** 2025-11-21

---

## Recent Notes
- 2025-11-21: Added global buffer cap + forced flush strategy in `ParquetWriter` to keep ingestion memory bounded on high-cardinality domains (OOM safeguard).
- 2025-11-20: Added `ingest_and_index_from_list.py` + `slurm_ingest_index.sh` for batch HuggingFace ingestion with resume + skip-existing detection (registry + Parquet presence). Flags allow forcing re-ingest.
- 2025-11-10: URL retrieval regression fixed (query now filters Parquet by canonical `domain`). Added targeted test to prevent reoccurrence.

---

## Milestones Snapshot
- **M1 Ingestion:** URL normalization, dataset ID registry, HuggingFace loader, parquet writer wiring.  
- **M2 Parquet Storage:** Partitioned `dataset_id` + `domain_prefix`, ZSTD level 6, buffered writes.  
- **M3 Index Build:** Domain dict, MPHF, membership (Roaring), postings, manifest. Incremental builder merges new files.  
- **M4 API:** FastAPI server + query service; MPHF + Roaring lookups; Parquet row-group fetch.  
- **M5 Incremental Updates:** Registry persistence, incremental postings/domains/membership; builder.auto merge new files.

---

## Key Components (pointers)
- Ingestion: `src/dataset_db/ingestion/{processor.py,hf_loader.py,dataset_registry.py}`
- Storage: `src/dataset_db/storage/{parquet_writer.py,layout.py}`
- Indexing: `src/dataset_db/index/{builder.py,domain_dict.py,membership.py,file_registry.py,postings.py,manifest.py}`
- API: `src/dataset_db/api/{server.py,loader.py,query.py}`
- Batch runner: `ingest_and_index_from_list.py`, `slurm_ingest_index.sh`

---

## How to Run (CLI quick path)
```bash
# Ingest a list from datasets.txt (default HF user, resume on, skip-existing on)
uv run python ingest_and_index_from_list.py --dataset-file datasets.txt

# Build indexes only (after data exists)
python - <<'PY'
from pathlib import Path
from dataset_db.index import IndexBuilder
builder = IndexBuilder(Path("./data"))
print("version:", builder.build_incremental())
PY

# Start API
uv run python examples/api_server.py
```

---

## Known Fixes / Guardrails
- Domain IDs remain stable across incremental builds (append-only domain merges).
- API query path uses correct file registry accessors and passes version into postings lookups.
- Loader state dict saved per batch to allow resume; cleared on success.
- Skip-existing check: registry entry + partitions present + no pending state dict.

---

## Future (Milestone 6 targets)
- Pre-aggregated counts for domains/datasets.
- True row-group pushdown via PyArrow/DuckDB for URL retrieval.
- S3 backend + validation suite for remote paths.
