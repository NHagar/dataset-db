# End-to-End Testing Guide

This guide explains how to test the dataset-db implementation with your real datasets.

## Quick Start

### Option 1: Test with HuggingFace Dataset

If you have datasets on HuggingFace (like `nhagar/reddit_urls`):

```bash
# Install dependencies (if using uv)
uv run python examples/e2e_test.py --dataset reddit --source huggingface

# Or with regular Python
python examples/e2e_test.py --dataset reddit --source huggingface --username nhagar
```

### Option 2: Test with Local File

If you have a local CSV or Parquet file with a `url` column:

```bash
# With CSV file
python examples/e2e_test.py --dataset my_dataset --source local --file /path/to/urls.csv

# With Parquet file
python examples/e2e_test.py --dataset my_dataset --source local --file /path/to/urls.parquet
```

### Option 3: Test Existing Data

If you already have data in `./data/`:

```bash
# Skip ingestion and test existing data
python examples/e2e_test.py --skip-ingestion
```

## What the Test Script Does

The end-to-end test script runs through the complete pipeline:

### 1. **Ingestion** (STEP 1)
- Loads URLs from your dataset
- Normalizes URLs (lowercases, canonicalizes, extracts domains)
- Generates IDs (domain_id, url_id, dataset_id)
- Writes to partitioned Parquet storage at `./data/urls/`
- Shows progress and throughput statistics

### 2. **Index Building** (STEP 2)
- Builds domain dictionary (sorted unique domains)
- Builds MPHF (minimal perfect hash function for domain → ID lookup)
- Builds membership index (domain → datasets bitmap)
- Builds postings index (domain, dataset → row groups)
- Creates versioned index at `./data/index/{version}/`

### 3. **Query Testing** (STEP 3)
- Loads indexes into memory
- Tests domain lookups
- Tests dataset membership queries
- Tests URL retrieval with pagination
- Shows sample results

### 4. **API Server** (STEP 4 - Optional)
- Starts FastAPI server on `http://localhost:8000`
- Provides REST API endpoints
- Offers interactive docs at `http://localhost:8000/docs`

### 5. **Validation** (Optional with `--validate`)
- Verifies Parquet files were created
- Verifies indexes exist and are loadable
- Tests query service functionality
- Tests URL reconstruction
- Reports pass/fail for each check

## Command-Line Options

```bash
python examples/e2e_test.py [OPTIONS]
```

### Data Source Options

- `--dataset NAME` - Dataset name (required unless `--skip-ingestion`)
- `--source {huggingface,local}` - Where to load data from
- `--file PATH` - Local file path (required for `--source local`)
- `--username NAME` - HuggingFace username (default: `nhagar`)
- `--suffix SUFFIX` - HuggingFace dataset suffix (default: `_urls`)

### Workflow Options

- `--skip-ingestion` - Skip ingestion step, use existing data
- `--incremental` - Use incremental index build (faster for updates)
- `--validate` - Run validation checks after testing
- `--server-only` - Only start the API server

## Examples

### Example 1: Full Test with HuggingFace Dataset

```bash
python examples/e2e_test.py \
  --dataset reddit \
  --source huggingface \
  --username nhagar \
  --validate
```

This will:
1. Download and ingest `nhagar/reddit_urls` from HuggingFace
2. Build indexes
3. Test queries
4. Run validation checks
5. Offer to start the API server

### Example 2: Test with Local CSV File

```bash
python examples/e2e_test.py \
  --dataset my_urls \
  --source local \
  --file ~/Downloads/urls.csv \
  --validate
```

Your CSV file must have a `url` column with URLs to process.

### Example 3: Add New Data Incrementally

```bash
# First ingestion
python examples/e2e_test.py --dataset dataset1 --source huggingface

# Add more data later
python examples/e2e_test.py --dataset dataset2 --source huggingface --incremental
```

The `--incremental` flag uses incremental index building, which is much faster than full rebuilds.

### Example 4: Test Existing Data

```bash
# Test the data you've already ingested
python examples/e2e_test.py --skip-ingestion --validate
```

### Example 5: Start API Server Only

```bash
# Just start the server (if indexes already exist)
python examples/e2e_test.py --server-only
```

## Testing the API

Once the API server is running, you can test it with curl:

### Get datasets for a domain

```bash
curl http://localhost:8000/v1/domain/example.com
```

Response:
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

### Get URLs for a domain in a specific dataset

```bash
curl "http://localhost:8000/v1/domain/example.com/datasets/1/urls?offset=0&limit=10"
```

Response:
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

### Interactive API Documentation

Visit `http://localhost:8000/docs` in your browser for interactive API documentation where you can test queries directly.

## File Structure After Running

After running the test, you'll have:

```
./data/
├── urls/                           # Parquet storage
│   └── dataset_id={id}/
│       └── domain_prefix={hh}/
│           └── part-*.parquet
└── index/                          # Indexes
    ├── manifest.json               # Current version pointer
    └── {version}/                  # e.g., 2025-10-27T21:56:16Z
        ├── domains.txt.zst         # Sorted domains
        ├── domains.mphf            # Hash lookup
        ├── domain_to_datasets.roar # Membership bitmaps
        ├── files.tsv.zst           # File registry
        └── postings/               # Postings index
            └── {shard}/
                ├── postings.idx.zst
                └── postings.dat.zst
```

## Understanding the Output

### Ingestion Output

```
Batch 1: 100,000 rows | buffered: 100,000, flushed: 0
```

- **buffered**: Rows held in memory waiting to be written
- **flushed**: Rows written to Parquet files

### Index Build Output

```
Domains indexed: 164,099
Domain-to-dataset mappings: 164,099
```

- Shows how many unique domains were found
- Shows index statistics

### Query Output

```
Query: example.com
  Domain ID: 12345
  Datasets: 2
    - Dataset 1: ~null URLs
```

- Shows which datasets contain the domain
- Lists sample URLs from each dataset

## Troubleshooting

### Error: "No module named 'dataset_db'"

Make sure you're running from the project root directory:

```bash
cd /path/to/dataset-db
python examples/e2e_test.py ...
```

### Error: "File must have a 'url' column"

Your local file must have a column named `url` with URLs. Example CSV:

```csv
url
https://example.com/page1
https://example.org/page2
```

### Error: "No indexes found"

Run ingestion and index building first:

```bash
python examples/e2e_test.py --dataset my_dataset --source huggingface
```

Or if you have existing Parquet files:

```bash
python examples/e2e_test.py --skip-ingestion
```

### Error: HuggingFace dataset not found

- Check the dataset name and username are correct
- Ensure you have internet connectivity
- Make sure the dataset exists on HuggingFace

## Performance Tips

1. **Batch Size**: For large datasets, the script processes in 100K row batches by default. Adjust in the code if needed.

2. **Incremental Builds**: Use `--incremental` when adding new data to existing indexes. It's 5-10x faster than full rebuilds.

3. **Testing Limits**: The HuggingFace ingestion is limited to 10 batches by default for testing. Remove this limit in the code for production use.

4. **Memory**: The system is designed for streaming, so it can handle datasets larger than memory.

## Next Steps

After validating your implementation works:

1. **Remove test limits**: Edit `examples/e2e_test.py` and remove the 10-batch limit
2. **Ingest full datasets**: Run without limits to process complete datasets
3. **Production deployment**: See [spec.md](spec.md) for production architecture
4. **Optimization**: See Milestone 6 in [IMPLEMENTATION.md](IMPLEMENTATION.md) for future optimizations

## Related Files

- [examples/e2e_test.py](examples/e2e_test.py) - Main test script
- [examples/parquet_ingestion.py](examples/parquet_ingestion.py) - Ingestion examples
- [examples/index_building.py](examples/index_building.py) - Index building examples
- [examples/api_server.py](examples/api_server.py) - API server runner
- [spec.md](spec.md) - Complete system specification
- [IMPLEMENTATION.md](IMPLEMENTATION.md) - Implementation progress log
