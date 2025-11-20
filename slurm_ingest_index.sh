#!/bin/bash

#SBATCH --account=p32491              # Allocation to charge
#SBATCH --partition=normal             # Quest partition/queue
#SBATCH --job-name=dataset_ingest     # Job name shown in squeue
#SBATCH --nodes=1                     # Number of nodes
#SBATCH --ntasks-per-node=1           # Tasks (CPUs) per node
#SBATCH --time=48:00:00               # Walltime hh:mm:ss
#SBATCH --mem=16G                     # Memory per node
#SBATCH --output=%x_%j.out            # Standard output log
#SBATCH --error=%x_%j.err             # Standard error log

set -euo pipefail

# Reset modules to a clean state
module purge

# Allow overriding the project root (defaults to submission directory)
PROJECT_ROOT="${PROJECT_ROOT:-${SLURM_SUBMIT_DIR:-$(pwd)}}"
cd "$PROJECT_ROOT"

# Configuration (override via env)
DATASET_FILE="${DATASET_FILE:-datasets.txt}"   # Text file with dataset names (one per line)
BASE_PATH="${BASE_PATH:-./data}"               # Storage root for ingested data + indexes
HF_USERNAME="${HF_USERNAME:-nhagar}"           # HuggingFace username prefix
SPLIT="${SPLIT:-train}"                        # Dataset split to stream
POSTINGS_SHARDS="${POSTINGS_SHARDS:-1024}"     # Index postings shards
DATASET_SUFFIX="${DATASET_SUFFIX:-}"           # Optional suffix to append (e.g., _urls)
RESUME="${RESUME:-1}"                          # Set to 0 to disable resume
SKIP_EXISTING="${SKIP_EXISTING:-1}"            # Set to 0 to always attempt ingestion
FORCE_REINGEST="${FORCE_REINGEST:-0}"          # Set to 1 to force re-ingestion even if data exists

if [[ ! -f "$DATASET_FILE" ]]; then
  echo "Dataset list not found: $DATASET_FILE"
  exit 1
fi

RESUME_FLAG="--resume"
if [[ "$RESUME" == "0" ]]; then
  RESUME_FLAG="--no-resume"
fi

SUFFIX_ARGS=()
if [[ -n "$DATASET_SUFFIX" ]]; then
  SUFFIX_ARGS=(--dataset-suffix "$DATASET_SUFFIX")
fi

SKIP_ARGS=()
if [[ "$FORCE_REINGEST" == "1" ]]; then
  SKIP_ARGS=(--force-reingest)
elif [[ "$SKIP_EXISTING" == "0" ]]; then
  SKIP_ARGS=(--no-skip-existing)
fi

echo "Starting ingestion + index build"
echo "  Project root:    $PROJECT_ROOT"
echo "  Dataset file:    $DATASET_FILE"
echo "  Base path:       $BASE_PATH"
echo "  HuggingFace user:$HF_USERNAME"
echo "  Split:           $SPLIT"
echo "  Resume:          $RESUME_FLAG"

uv run python ingest_and_index_from_list.py \
  --dataset-file "$DATASET_FILE" \
  --username "$HF_USERNAME" \
  --base-path "$BASE_PATH" \
  --split "$SPLIT" \
  --postings-shards "$POSTINGS_SHARDS" \
  "${SUFFIX_ARGS[@]}" \
  "${SKIP_ARGS[@]}" \
  $RESUME_FLAG
