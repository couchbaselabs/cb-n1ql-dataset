#!/bin/bash
# ============================================================
# PromptSQL — End-to-end Couchbase SQL++ generation pipeline
# ============================================================
#
# Usage:
#   ./run.sh                   # run full pipeline
#   ./run.sh --limit 5         # test with 5 instances only
#
# Prerequisites:
#   1. Copy .env.example to .env and fill in your API key
#   2. pip install -r requirements.txt
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# Parse --limit argument (default: 0 = all)
LIMIT=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --limit) LIMIT="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

echo "=========================================="
echo "  PromptSQL++ Pipeline"
echo "=========================================="
echo ""

# Step 1: Preprocess
echo "▶ Step 1/4: Preprocess (load instances + schemas)"
python preprocess.py
echo ""

# Step 2: Build prompts
echo "▶ Step 2/4: Build prompts"
python build_prompt.py
echo ""

# Step 3: Ask LLM
echo "▶ Step 3/4: Ask LLM"
LIMIT_ARG=""
if [ "$LIMIT" -gt 0 ]; then
    LIMIT_ARG="--limit $LIMIT"
fi
python ask_llm.py $LIMIT_ARG --skip_existing
echo ""

# Step 4: Postprocess
echo "▶ Step 4/4: Postprocess"
python postprocess.py
echo ""

echo "=========================================="
echo "  Pipeline complete!"
echo "  Submission files: submission/"
echo "=========================================="
