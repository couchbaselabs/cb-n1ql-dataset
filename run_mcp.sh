#!/bin/bash
# ============================================================
# MCP-based pipeline (generate → postprocess → evaluate → analyze)
# ============================================================
#
# Usage:
#   ./run_mcp.sh                          # full pipeline (sqlite mode)
#   ./run_mcp.sh --mode bird              # use bird questions
#   ./run_mcp.sh --mode snowflake         # use snowflake questions
#   ./run_mcp.sh --tag my_run             # custom run name suffix
#   ./run_mcp.sh --skip_eval              # generate only, skip eval
#   ./run_mcp.sh --eval_only              # evaluate existing submission
#
# Mode → questions file:
#   sqlite    → test/questions_sqlite.json
#   snowflake → test/questions_snowflake.json
#   bird      → test/questions_bird.json
#
# All outputs organized under: runs/mcp[_<tag>]/
#   ├── run_meta.json       — timestamps, scores
#   ├── submission/         — .sqlpp files
#   └── logs/
#       ├── log_sqlpp_catalog.jsonl — structured eval log
#       ├── evaluate.log            — raw eval console output
#       └── analysis_report.txt
# ============================================================

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASELINE_DIR="${ROOT_DIR}/baselines/promptSQL++"
EVAL_DIR="${ROOT_DIR}/evaluation_pipeline"
GOLD_DIR="${EVAL_DIR}/gold"
RUNS_DIR="${ROOT_DIR}/runs"
TEST_DIR="${ROOT_DIR}/test"
QUERIES_DIR="${TEST_DIR}/output/queries"


PYTHON_BIN="$(command -v python3 || command -v python || true)"
if [[ -z "$PYTHON_BIN" ]]; then
    echo "Error: python3 not found in PATH"
    exit 1
fi

# ---------- Defaults ----------
MODE="sqlite"
RUN_TAG=""
SKIP_EVAL=false
EVAL_ONLY=false
MAX_WORKERS=1
TIMEOUT=360
LIMIT=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)        MODE="$2"; shift 2 ;;
        --tag)         RUN_TAG="$2"; shift 2 ;;
        --skip_eval)   SKIP_EVAL=true; shift ;;
        --eval_only)   EVAL_ONLY=true; shift ;;
        --max_workers) MAX_WORKERS="$2"; shift 2 ;;
        --timeout)     TIMEOUT="$2"; shift 2 ;;
        --limit)       LIMIT="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

case "$MODE" in
    sqlite)    QUESTIONS_FILE="${TEST_DIR}/questions_sqlite.json" ;;
    snowflake) QUESTIONS_FILE="${TEST_DIR}/questions_snowflake.json" ;;
    bird)      QUESTIONS_FILE="${TEST_DIR}/questions_bird.json" ;;
    *)         echo "Error: --mode must be sqlite, snowflake, or bird (got '$MODE')"; exit 1 ;;
esac

if [[ ! -f "$QUESTIONS_FILE" ]]; then
    echo "Error: questions file not found for mode '$MODE': $QUESTIONS_FILE"
    exit 1
fi

# ---------- Build run name ----------
RUN_NAME="mcp"
if [[ -n "$RUN_TAG" ]]; then
    RUN_NAME="mcp_${RUN_TAG}"
fi

RUN_DIR="${RUNS_DIR}/${RUN_NAME}"
SUBMISSION_DIR="${RUN_DIR}/submission"
LOG_DIR="${RUN_DIR}/logs"

mkdir -p "$SUBMISSION_DIR" "$LOG_DIR"

echo "=========================================="
echo "  MCP Pipeline"
echo "=========================================="
echo "  Mode:          $MODE"
echo "  Questions:     $QUESTIONS_FILE"
echo "  Run name:      $RUN_NAME"
echo "  Run dir:       $RUN_DIR"
echo "=========================================="
echo ""

cat > "${RUN_DIR}/run_meta.json" <<EOF
{
    "pipeline": "mcp",
    "pipeline_mode": "$MODE",
    "run_name": "$RUN_NAME",
    "run_tag": "$RUN_TAG",
    "questions_file": "$QUESTIONS_FILE",
    "started_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

# ============================================================
# STEP 1: Generate SQL++ via MCP
# ============================================================
if [[ "$EVAL_ONLY" == "false" ]]; then
    echo "▶ Step 1/3: Generate SQL++ queries via MCP"
    LIMIT_ARG=""
    if [[ "$LIMIT" -gt 0 ]]; then
        LIMIT_ARG="--limit $LIMIT"
        echo "  (limit: $LIMIT questions)"
    fi
    "$PYTHON_BIN" "${TEST_DIR}/run.py" --questions_file "$QUESTIONS_FILE" $LIMIT_ARG
    echo ""

    # ============================================================
    # STEP 2: Postprocess — copy queries to submission dir
    # ============================================================
    echo "▶ Step 2/3: Postprocess — copying queries to submission dir"
    cp "${QUERIES_DIR}"/*.sqlpp "${SUBMISSION_DIR}/" 2>/dev/null || true
    COUNT=$(ls "${SUBMISSION_DIR}"/*.sqlpp 2>/dev/null | wc -l | tr -d ' ')
    echo "  Copied ${COUNT} .sqlpp files to ${SUBMISSION_DIR}"
    echo ""
else
    echo "▶ Skipping Steps 1-2 (--eval_only)"
    echo "  Using existing submission: $SUBMISSION_DIR"
    echo ""
fi

# ============================================================
# STEP 3: Evaluate against Couchbase
# ============================================================
if [[ "$SKIP_EVAL" == "false" ]]; then
    echo "▶ Step 3/3: Evaluate against Couchbase"

    "$PYTHON_BIN" "${EVAL_DIR}/evaluate_sqlpp_catalog.py" \
        --result_dir "$SUBMISSION_DIR" \
        --gold_dir "$GOLD_DIR" \
        --max_workers "$MAX_WORKERS" \
        --timeout "$TIMEOUT" \
        2>&1 | tee "${LOG_DIR}/evaluate.log"

    if [[ -f "${EVAL_DIR}/log_sqlpp_catalog.jsonl" ]]; then
        cp "${EVAL_DIR}/log_sqlpp_catalog.jsonl" "${LOG_DIR}/log_sqlpp_catalog.jsonl"
    fi

    echo ""
    echo "▶ Analyzing evaluation log..."
    "$PYTHON_BIN" "${EVAL_DIR}/analyze_log.py" 2>&1 | tee "${LOG_DIR}/analysis_report.txt" || true

    if [[ -f "${EVAL_DIR}/analysis_report.txt" ]]; then
        cp "${EVAL_DIR}/analysis_report.txt" "${LOG_DIR}/analysis_report.txt" 2>/dev/null || true
    fi

    echo ""
else
    echo "▶ Skipping Step 3 (--skip_eval)"
    echo ""
fi

# ---------- Update run metadata with scores ----------
"$PYTHON_BIN" -c "
import json, os
meta_path = '${RUN_DIR}/run_meta.json'
with open(meta_path) as f:
    meta = json.load(f)
meta['completed_at'] = '$(date -u +%Y-%m-%dT%H:%M:%SZ)'
log_path = '${LOG_DIR}/log_sqlpp_catalog.jsonl'
if os.path.exists(log_path):
    with open(log_path) as f:
        for line in f:
            try:
                entry = json.loads(line)
                if entry.get('event') == 'real_score':
                    meta['real_score'] = entry.get('score')
                    meta['correct'] = entry.get('correct')
                    meta['total_local'] = entry.get('total_local')
                if entry.get('event') == 'final_score':
                    meta['final_score'] = entry.get('score')
            except: pass
with open(meta_path, 'w') as f:
    json.dump(meta, f, indent=2)
"

echo "=========================================="
echo "  Pipeline complete!  [${RUN_NAME}]"
echo "=========================================="
echo "  Run directory: ${RUN_DIR}"
echo "  Submission:    ${SUBMISSION_DIR}"
echo "  Logs:          ${LOG_DIR}"
echo "=========================================="
