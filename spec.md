# Implementation Spec: Domain → Datasets → URLs (at extreme scale)

## 0) Scope

Input: many datasets, each with records `{url, [ts], [extras...]}`.
Queries you must support:

1. `GET /domain/{d}` → list datasets containing `d` (+ counts).
2. `GET /domain/{d}/dataset/{id}/urls?offset=&limit=` → URLs for `d` in dataset `id`.

Targets: minimal disk, fast lookup, incremental updates, atomic versioning.

---

## 1) Canonicalization & IDs

### 1.1 URL & domain normalization

* Parse with a robust library; strip fragments.
* Lowercase scheme/host; percent-decode path/query minimally; drop default ports.
* Extract **eTLD+1** via Public Suffix List (PSL). Convert to **punycode**.
* Preserve a raw copy if needed for provenance.

**Pseudocode**

```python
def norm_url(u):
    p = parse(u)
    host = to_punycode(lower(p.host))
    scheme = lower(p.scheme or "http")
    port = None if p.port in (None, default_port(scheme)) else p.port
    path = normalize_path(p.path)        # collapse //, remove /./, keep trailing slash
    query = normalize_query(p.query)     # sort keys; preserve dup keys if needed
    fragment = None                      # drop
    etld1 = public_suffix_eTLD1(host)
    return NormURL(
        scheme=scheme, host=host, port=port, path=path, query=query,
        domain=etld1, raw=u
    )
```

### 1.2 ID schemes

* `dataset_id`: `UInt32`, assigned via registry.
* `domain_id`: MPHF index over **sorted unique domain strings** (BBHash); fallback to (hash64, small collision map) if you prefer simpler builds.
* `url_id`: `xxh3_64(raw_url_bytes)`.

  * Collision guard: if collision detected, spill a `(url_id -> full_url)` mini-bucket; keep a 16-bit tag (`xxh3_64 >> 48`) to early-reject mismatches.

---

## 2) Storage layout (object storage)

### 2.1 Parquet “truth” lake

**Directory**

```
s3://{bucket}/urls/
  dataset_id={id}/
    domain_prefix={hh}/           # hh = first 2 hex of domain_hash(domain_str)
      part-00000.parquet
      part-00001.parquet
```

**Schema (Parquet)**

```
dataset_id:   INT32
domain_id:    INT64        # or UINT64 via logical type
url_id:       INT64
scheme:       BYTE_ARRAY   (DICTIONARY)
host_id:      INT64        # optional, if you dictionary hosts separately
path_query:   BYTE_ARRAY   (DICTIONARY)  # "/a/b?x=1&y=2"
ts:           TIMESTAMP    # optional
extras:       ...          # optional
```

**Encoding**

* **ZSTD** (level 6–9), dictionary encoding enabled.
* Row group target: **128MB** (tune 64–256MB).
* Partitioning: `dataset_id`, `domain_prefix` (2 hex chars).

  * Benefits: Parquet min/max stats + partition pruning reduce IO explosively.

### 2.2 Sidecar indexes

All paths versioned under a **manifest** (see §4).

**A) Domain dictionary**

```
s3://{bucket}/index/{version}/domains.txt.zst        # sorted unique domains, \n-delimited
s3://{bucket}/index/{version}/domains.mphf           # BBHash structures
```

* `domains.txt.zst` supports reverse lookup (id → string).
* `domains.mphf` supports fast lookup (string → id).

**B) domain → datasets membership (Roaring)**
Single memory-mappable file with a compact index.

```
s3://{bucket}/index/{version}/domain_to_datasets.roar
```

**Format**

```
[magic=DTDR][ver=1][N_domains:uint64][index_offset:uint64]
[bitmaps... concatenated]
[index: N_domains entries of {bitmap_start:uint64, bitmap_len:uint32}]
```

* Each bitmap = **Roaring** serialized bytes for the set of `dataset_id`s containing that domain.
* Use croaring / roaring-rs, etc.

**C) Postings: (domain_id, dataset_id) → row-group pointers**
Prefer **offsets over URL IDs** to avoid duplication and let Parquet do the heavy lifting.

```
s3://{bucket}/index/{version}/postings/
  {shard}/
    postings.idx.zst
    postings.dat.zst
```

* **Sharding**: `shard = (domain_id % 1024)` or two-level shard by `(domain_prefix, dataset_bucket)`.
* `postings.idx.zst` (sorted by (domain_id, dataset_id)):

  ```
  [magic=PDX1][ver=1][N:uint64][dat_offset:uint64]
  repeat N times:
    domain_id:uint64, dataset_id:uint32, payload_offset:uint64, payload_len:uint32
  ```
* `postings.dat.zst` stores a sequence of payloads; each payload = **varint-encoded** list of `(file_id:uint32, row_group:uint32)` tuples.

  * `file_id` resolved via a small **file registry**:

    ```
    s3://{bucket}/index/{version}/files.tsv.zst
    # file_id \t dataset_id \t domain_prefix \t parquet_rel_path
    ```

*If you prefer URL-ID postings:* store varint delta-encoded sorted `url_id`s instead of row-group tuples; then fetch URL strings via a URL dictionary (more dedupe, more plumbing).

---

## 3) ETL / Build pipeline

### 3.1 Ingest batch (Spark/Polars)

* Read raw dataset.
* Normalize URLs → `(dataset_id, domain_str, domain_id, url_id, scheme, host_id?, path_query, ts?, extras...)`.
* Write Parquet to the partitioned layout (target row group size).

### 3.2 Index build (Rust recommended)

* **Domains**: extract unique `domain_str` (from new batch), merge with previous, rebuild `domains.txt.zst` + `domains.mphf` (or maintain incremental MPHF using a two-level scheme).
* **Membership**: for each `domain_id` touched, add `dataset_id` to its Roaring bitmap; serialize into `domain_to_datasets.roar` (copy-on-write rebuild or chunk-level append + compaction).
* **Postings**: scan only **new** Parquet row groups; for each row group, get distinct `(domain_id, dataset_id)` pairs; append payload `(file_id,row_group)` into postings. Periodically **merge** and **re-order** payloads so each (domain, dataset) has one contiguous payload.

### 3.3 Publish new manifest (atomic)

```
s3://{bucket}/index/manifest.json
{
  "current_version": "2025-10-20T22:00:00Z",
  "versions": [
    {
      "version": "2025-10-20T22:00:00Z",
      "domains_txt": "index/2025-10-20/domains.txt.zst",
      "domains_mphf": "index/2025-10-20/domains.mphf",
      "d2d_roar": "index/2025-10-20/domain_to_datasets.roar",
      "postings_idx": "index/2025-10-20/postings/{shard}/postings.idx.zst",
      "postings_dat": "index/2025-10-20/postings/{shard}/postings.dat.zst",
      "files_tsv":  "index/2025-10-20/files.tsv.zst",
      "parquet_root": "urls/"
    }
  ]
}
```

* Writers produce a new `version` in full, then atomically flip `manifest.json.current_version`.

---

## 4) Serving layer

### 4.1 Process layout

* **Warm state (memory-mapped):**

  * `domains.mphf`
  * `domain_to_datasets.roar` + small LRU for hot bitmaps
  * LRU for postings idx pages (shard-paged)
* **Cold via HTTPFS/S3**:

  * Parquet row groups you need for a request (DuckDB/Polars can read remote byte ranges)
  * Postings payloads (dat.zst) lazily fetched by byte range

### 4.2 API (example)

```
GET /v1/domain/{domain}
→ 200 {
  "domain": "example.com",
  "domain_id": 123456,
  "datasets": [
    {"dataset_id": 17, "url_count_est": 15823},  # from sketches or pre-agg
    ...
  ]
}

GET /v1/domain/{domain}/datasets/{dataset_id}/urls?offset=0&limit=1000
→ 200 {
  "domain": "example.com",
  "dataset_id": 17,
  "total_est": 15823,
  "items": [
    {"url_id": 0x9bc..., "url": "https://example.com/a/b?x=1", "ts": "2024-06-14T..."},
    ...
  ],
  "next_offset": 1000
}
```

### 4.3 Query algorithm (row-group offsets flavor)

```python
def get_datasets_for_domain(domain_str):
    did = mphf_lookup(domain_str)
    if did is None: return []
    bm = roaring_fetch_bitmap(did)                 # memory-mapped + small memcpy
    ds_ids = bm.to_list()                           # or iterate
    counts = sketch_get_counts(did, ds_ids)         # optional HLL/top-K store
    return [{"dataset_id": d, "url_count_est": counts.get(d)} for d in ds_ids]

def get_urls_for_domain_dataset(domain_str, dataset_id, offset, limit):
    did = mphf_lookup(domain_str)
    entry = postings_locate(did, dataset_id)       # binary search postings.idx shard
    if not entry: return [], 0
    payload = postings_read(entry.offset, entry.len) # varint decode list[(file_id, rg)]
    # Flatten the row-groups, compute which row-groups cover [offset:offset+limit)
    remaining = limit
    out = []
    for (file_id, rg) in payload.covering(offset, limit):
        file_meta = files_registry[file_id]
        # Read ONLY row group rg, projecting required columns
        batch = scan_parquet_row_group(file_meta, rg, predicate=domain_id==did)
        # slice according to leftover offset within this rg
        batch = batch.slice(local_offset, remaining)
        out.extend(reconstruct_urls(batch))
        remaining -= len(batch)
        if remaining <= 0: break
    return out
```

**Notes**

* Keep a small **file registry cache** (file_id → S3 path).
* Use **HEAD + Range** requests to read only row-group byte ranges.
* If you need exact `total`, store a pre-aggregated count per (domain,dataset) (see §5).

---

## 5) Pre-aggregations & sketches (optional but recommended)

* `(domain_id, dataset_id) → url_count` table (tiny Delta/Iceberg table or a KV).
* `(domain_id, dataset_id) → top_paths` (small heap or count-min sketch for the UI).
* `(domain_id) → HLL(dataset_id)` only if you want unique dataset counts (often redundant with bitmap size).

---

## 6) Incremental updates, compaction, versioning

* **Write new datasets** or **new day partitions** → append new Parquet files.
* **Index updates**:

  * Append new row-group offsets to `(domain,dataset)` postings; if an entry becomes fragmented, **periodically compact** into one payload per key.
  * Roaring bitmaps updated with set-add (immutable rebuild per version).
* **Versioning**: each ETL publish produces a fresh `version` directory; **never mutate** previous versions → readers choose `manifest.current_version`.
* **GC**: keep last N versions; background job removes old versions once no reader references them.

---

## 7) Performance & sizing defaults

* Row group: 128MB, Parquet page size default, ZSTD level 6–9.
* Shards: 1024 for postings; keeps index binary searches short and payload files small.
* LRU caches:

  * 64–256MB for postings idx pages
  * 32–128MB for Roaring bitmap slices
* Concurrency:

  * Async prefetch postings payload + row groups
  * Cap per-request row groups (e.g., ≤32) and page results

---

## 8) Access control & governance

* **Dataset ACLs**: keep a `dataset_acl` table keyed by dataset_id → allowed principals.
* At request time, filter `ds_ids` from bitmap by ACL.
* Log only **domain_id, dataset_id, counts**; avoid logging raw URLs unless needed (PII risk).

---

## 9) Failure modes & mitigations

* **MPHF mismatch**: embed hash of `domains.txt.zst` in `domains.mphf`; server verifies on load.
* **S3 partial reads**: retry with exponential backoff; validate payload CRC (wrap payloads with a 32-bit CRC header).
* **Hotspot domains**: cache full `(domain→datasets)` and top postings payloads; consider pre-materialized “top 1k URLs” per (domain,dataset).
* **Large result sets**: enforce pagination & `limit` caps; add server-side timeouts per step.

---

## 10) Tooling & libraries (battle-tested picks)

* **Parquet/Columnar**: DuckDB (HTTPFS), PyArrow/Polars, ClickHouse (Alt).
* **Roaring**: croaring (C), roaring-rs (Rust), roaringbitmap (Go/Java).
* **MPHF**: BBHash (C++), mphf (Rust).
* **Hashing**: xxhash/xxh3.
* **Compression**: zstd.

---

## 11) Testing checklist

* **Correctness**: random sample domains → recompute from raw files; counts match.
* **Collision tests**: simulate hash collisions; ensure bucket spill works.
* **Latency SLOs**:

  * `/domain/{d}`: p95 < 30ms (warm)
  * `/domain/{d}/dataset/{id}/urls`: first page p95 < 200ms (warm), < 1s cold
* **Soak/load**: Zipf domain distribution; sustain N RPS with λ≈Pareto heavy tails.
* **Chaos**: flip manifest mid-traffic; ensure readers pin version until request completes.

---

## 12) Minimal milestones (build order)

1. **Parquet writer** with normalization + partitioning.
2. **Membership index** (domain→datasets Roaring) + lookup endpoint.
3. **Postings (offsets)** + first page URL streaming.
4. **Counts MV** (or sketch) for instant numbers in the UI.
5. **Incremental publish** + manifest flip + GC.
6. **Caches & compaction** for hotspots and long tails.
