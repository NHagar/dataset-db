# Quick Start Guide

Get started with dataset-db in 5 minutes!

## Prerequisites

- Python 3.9+
- `uv` package manager (or regular Python with pip)

## Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd dataset-db

# Install dependencies
uv pip install -e .
# or with regular pip:
# pip install -e .
```

## Quick Test with Existing Data

You already have some test data! Let's inspect it:

```bash
# See what data you have
python examples/inspect_data.py
```

This will show:
- How many Parquet files exist
- What datasets are present
- Whether indexes are built
- Sample data

## Test End-to-End with Your Own Data

### Option 1: HuggingFace Dataset

If you have a dataset on HuggingFace (like `nhagar/reddit_urls`):

```bash
python examples/e2e_test.py \
  --dataset reddit_urls \
  --source huggingface \
  --username nhagar
```

### Option 2: Local File

If you have a CSV or Parquet file with URLs:

```bash
python examples/e2e_test.py \
  --dataset my_data \
  --source local \
  --file /path/to/urls.csv
```

Your file must have a `url` column.

### Option 3: Use Existing Data

If you want to test the current data in `./data/`:

```bash
python examples/e2e_test.py --skip-ingestion
```

## What Happens During the Test

The test script will:

1. **Ingest** URLs and normalize them
2. **Build** indexes (domain dictionary, MPHF, membership, postings)
3. **Test** queries to verify everything works
4. **Offer** to start the API server

## Using the API

After the test completes, you can start the API server:

```bash
python examples/e2e_test.py --server-only
```

Then query it:

```bash
# Get datasets for a domain
curl http://localhost:8000/v1/domain/example.com

# Get URLs for a domain in a specific dataset
curl "http://localhost:8000/v1/domain/example.com/datasets/1/urls?limit=10"
```

Or visit the interactive docs: `http://localhost:8000/docs`

## Query a Specific Domain

```bash
python examples/inspect_data.py --domain example.com
```

This shows:
- Which datasets contain the domain
- Sample URLs from each dataset

## File Structure

```
./data/
├── urls/                    # Parquet storage (normalized URLs)
│   └── dataset_id={id}/
│       └── domain_prefix={hh}/
│           └── part-*.parquet
└── index/                   # Indexes for fast lookups
    ├── manifest.json
    └── {version}/
        ├── domains.txt.zst
        ├── domains.mphf
        ├── domain_to_datasets.roar
        └── postings/
```

## Example Workflow

Here's a complete workflow with a real dataset:

```bash
# 1. Ingest data from HuggingFace
python examples/e2e_test.py \
  --dataset reddit_urls \
  --source huggingface \
  --username nhagar

# 2. Inspect what was created
python examples/inspect_data.py --details

# 3. Query a domain
python examples/inspect_data.py --domain reddit.com

# 4. Add more data (incremental)
python examples/e2e_test.py \
  --dataset twitter_urls \
  --source huggingface \
  --username nhagar \
  --incremental

# 5. Start the API server
python examples/e2e_test.py --server-only
```

## Common Commands

```bash
# Inspect existing data
python examples/inspect_data.py

# Test with validation
python examples/e2e_test.py --skip-ingestion --validate

# Add new data incrementally (faster)
python examples/e2e_test.py --dataset new_data --source local --file data.csv --incremental

# Start API server
python examples/e2e_test.py --server-only

# Run linting
uv run ruff check . --fix
```

## Troubleshooting

### No data found

Run ingestion first:

```bash
python examples/e2e_test.py --dataset test --source huggingface
```

### No indexes found

Build indexes from existing data:

```bash
python examples/e2e_test.py --skip-ingestion
```

### Import errors

Make sure you're in the project root and dependencies are installed:

```bash
cd /path/to/dataset-db
uv pip install -e .
```

## Next Steps

- **Full documentation**: See [E2E_TESTING.md](E2E_TESTING.md)
- **Implementation details**: See [IMPLEMENTATION.md](IMPLEMENTATION.md)
- **System specification**: See [spec.md](spec.md)
- **Run tests**: `uv run pytest tests/`
- **Production deployment**: See spec.md for scaling considerations

## Getting Help

- Check [E2E_TESTING.md](E2E_TESTING.md) for detailed testing instructions
- Review [IMPLEMENTATION.md](IMPLEMENTATION.md) for technical details
- Look at example scripts in [examples/](examples/)
- Run `python examples/e2e_test.py --help` for all options
