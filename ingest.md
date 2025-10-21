We're running ingestion from a large series of huggingface datasets. Each dataset follows the naming convention nhagar/[dataset_name]_urls. It uses this schema:

url: string - record URL
domain: string - top-level domain of record URL

Each dataset is stored as a set of Parquet files. 