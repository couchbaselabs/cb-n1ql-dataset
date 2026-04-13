#!/bin/bash
# ============================================================
# PromptSQL++ — End-to-end pipeline (generate → evaluate → analyze)
# ============================================================
#
# Usage:
#   ./run.sh                                         # full pipeline (model from .env)
#   ./run.sh --model claude-opus-4.6 --provider openai
#   ./run.sh --model gemini-3.1-pro  --provider openai
#   ./run.sh --limit 5                               # test with N instances
#   ./run.sh --questions_file baselines/promptSQL++/prompts/questions_failed_only.json
#   ./run.sh --skip_eval                             # generate only, skip eval
#   ./run.sh --eval_only                             # evaluate existing submissions
#   ./run.sh --tag v2                                # custom run tag
#
# All outputs organized under: runs/<model_name>/
#   ├── run_meta.json       — model, timestamps, scores
#   ├── raw_output/         — raw LLM responses
#   ├── submission/         — cleaned .sqlpp files
#   └── logs/
#       ├── log_sqlpp.jsonl — structured eval log
#       ├── evaluate.log    — raw eval console output
#       └── analysis_report.txt
#
# Prerequisites:
#   1. Copy baselines/promptSQL++/.env.example to .env and fill in API keys
#   2. pip install -r baselines/promptSQL++/requirements.txt
# ============================================================

set -euo pipefail

# ---------- Directories ----------
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASELINE_DIR="${ROOT_DIR}/baselines/promptSQL++"
EVAL_DIR="${ROOT_DIR}/evaluation_pipeline"
GOLD_DIR="${EVAL_DIR}/gold"
RUNS_DIR="${ROOT_DIR}/runs"

# ---------- Load .env ----------
ENV_FILE="${BASELINE_DIR}/.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

# ---------- Defaults from .env ----------
DEFAULT_PROVIDER="${LLM_PROVIDER:-openai}"
DEFAULT_MODEL="${LLM_MODEL:-gpt-5.1}"

# ---------- Python interpreter ----------
PYTHON_BIN="$(command -v python || command -v python3 || true)"
if [[ -z "$PYTHON_BIN" ]]; then
    echo "Error: neither 'python' nor 'python3' was found in PATH"
    exit 1
fi

# ---------- Parse arguments ----------
LIMIT=0
MODE="sqlite"
PROVIDER="${DEFAULT_PROVIDER}"
MODEL="${DEFAULT_MODEL}"
PROMPT_MODE="basic"
SKIP_EVAL=false
EVAL_ONLY=false
RUN_TAG=""
THINKING=false
THINKING_EFFORT="high"
QUESTIONS_FILE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --limit)       LIMIT="$2"; shift 2 ;;
        --mode)        MODE="$2"; shift 2 ;;
        --provider)    PROVIDER="$2"; shift 2 ;;
        --model)       MODEL="$2"; shift 2 ;;
        --prompt_mode) PROMPT_MODE="$2"; shift 2 ;;
        --skip_eval)   SKIP_EVAL=true; shift ;;
        --eval_only)   EVAL_ONLY=true; shift ;;
        --tag)         RUN_TAG="$2"; shift 2 ;;
        --thinking)    THINKING=true; shift ;;
        --thinking_effort) THINKING=true; THINKING_EFFORT="$2"; shift 2 ;;
        --questions_file) QUESTIONS_FILE="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# Validate mode
if [[ "$MODE" != "sqlite" && "$MODE" != "bigquery" && "$MODE" != "snowflake" ]]; then
    echo "Error: --mode must be 'sqlite', 'bigquery', or 'snowflake' (got '$MODE')"
    exit 1
fi

if [[ -n "$QUESTIONS_FILE" ]]; then
    if [[ "$QUESTIONS_FILE" != /* ]]; then
        QUESTIONS_FILE="${ROOT_DIR}/${QUESTIONS_FILE}"
    fi
    if [[ ! -f "$QUESTIONS_FILE" ]]; then
        echo "Error: --questions_file not found: $QUESTIONS_FILE"
        exit 1
    fi
fi

QUESTIONS_FILE_JSON="null"
if [[ -n "$QUESTIONS_FILE" ]]; then
    QUESTIONS_FILE_JSON="\"$QUESTIONS_FILE\""
fi

# ---------- Build run name ----------
MODEL_SLUG=$(echo "$MODEL" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9._-]/_/g')
if [[ "$THINKING" == "true" ]]; then
    MODEL_SLUG="${MODEL_SLUG}_thinking_${THINKING_EFFORT}"
fi
if [[ -n "$RUN_TAG" ]]; then
    RUN_NAME="${MODEL_SLUG}_${RUN_TAG}"
else
    RUN_NAME="${MODEL_SLUG}"
fi

# ---------- Create output directories ----------
RUN_DIR="${RUNS_DIR}/${RUN_NAME}"
RAW_OUTPUT_DIR="${RUN_DIR}/raw_output"
SUBMISSION_DIR="${RUN_DIR}/submission"
LOG_DIR="${RUN_DIR}/logs"

mkdir -p "$RAW_OUTPUT_DIR" "$SUBMISSION_DIR" "$LOG_DIR"

echo "=========================================="
echo "  PromptSQL++ Pipeline"
echo "=========================================="
echo "  Provider:      $PROVIDER"
echo "  Model:         $MODEL"
echo "  Prompt mode:   $PROMPT_MODE"
echo "  Pipeline mode: $MODE"
echo "  Run name:      $RUN_NAME"
echo "  Run dir:       $RUN_DIR"
echo "=========================================="
echo ""

# ---------- Save run metadata ----------
cat > "${RUN_DIR}/run_meta.json" <<EOF
{
    "model": "$MODEL",
    "provider": "$PROVIDER",
    "prompt_mode": "$PROMPT_MODE",
    "pipeline_mode": "$MODE",
    "run_name": "$RUN_NAME",
    "run_tag": "$RUN_TAG",
    "thinking": $THINKING,
    "thinking_effort": "$THINKING_EFFORT",
    "questions_file": $QUESTIONS_FILE_JSON,
    "limit": $LIMIT,
    "started_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

# ============================================================
# STEP 1-4: Generate SQL++ queries (runs inside baselines dir)
# ============================================================
if [[ "$EVAL_ONLY" == "false" ]]; then

    if [[ -n "$QUESTIONS_FILE" ]]; then
        echo "▶ Step 1/5: Using custom questions file"
        echo "  Skipping preprocess/build_prompt"
        echo "  Questions file: $QUESTIONS_FILE"
        echo ""
    else
        # Step 1: Preprocess
        echo "▶ Step 1/5: Preprocess (load instances + schemas)"
        "$PYTHON_BIN" "${BASELINE_DIR}/preprocess.py" --mode "$MODE"
        echo ""

        # Step 2: Build prompts
        echo "▶ Step 2/5: Build prompts (${PROMPT_MODE})"
        "$PYTHON_BIN" "${BASELINE_DIR}/build_prompt.py" --prompt_mode "$PROMPT_MODE" --mode "$MODE"
        echo ""
    fi

    # Step 3: Ask LLM
    echo "▶ Step 3/5: Ask LLM ($PROVIDER / $MODEL)"
    LIMIT_ARG=""
    if [ "$LIMIT" -gt 0 ]; then
        LIMIT_ARG="--limit $LIMIT"
    fi
    THINKING_ARG=""
    if [[ "$THINKING" == "true" ]]; then
        THINKING_ARG="--thinking --thinking_effort $THINKING_EFFORT"
    fi
    ASK_LLM_CMD=(
        "$PYTHON_BIN" "${BASELINE_DIR}/ask_llm.py"
        --provider "$PROVIDER"
        --model "$MODEL"
        --output_dir "$RAW_OUTPUT_DIR"
        --skip_existing
    )
    if [[ -n "$QUESTIONS_FILE" ]]; then
        ASK_LLM_CMD+=(--input "$QUESTIONS_FILE")
    fi
    if [ "$LIMIT" -gt 0 ]; then
        ASK_LLM_CMD+=(--limit "$LIMIT")
    fi
    if [[ "$THINKING" == "true" ]]; then
        ASK_LLM_CMD+=(--thinking --thinking_effort "$THINKING_EFFORT")
    fi
    "${ASK_LLM_CMD[@]}"
    echo ""

    # Step 4: Postprocess
    echo "▶ Step 4/5: Postprocess"
    "$PYTHON_BIN" "${BASELINE_DIR}/postprocess.py" \
        --input_dir "$RAW_OUTPUT_DIR" \
        --output_dir "$SUBMISSION_DIR"
    echo ""

else
    echo "▶ Skipping Steps 1-4 (--eval_only mode)"
    echo "  Using existing submission: $SUBMISSION_DIR"
    echo ""
fi

# ============================================================
# STEP 5: Evaluate against Couchbase
# ============================================================
if [[ "$SKIP_EVAL" == "false" ]]; then
    echo "▶ Step 5/5: Evaluate against Couchbase"

    "$PYTHON_BIN" "${EVAL_DIR}/evaluate_sqlpp_sqlite.py" \
        --mode sql \
        --result_dir "$SUBMISSION_DIR" \
        --gold_dir "$GOLD_DIR" \
        --max_workers 1 \
        --timeout 1200 \
        2>&1 | tee "${LOG_DIR}/evaluate.log"

    # Copy the evaluation JSONL log to the run's log directory
    if [[ -f "${EVAL_DIR}/log_sqlpp.jsonl" ]]; then
        cp "${EVAL_DIR}/log_sqlpp.jsonl" "${LOG_DIR}/log_sqlpp.jsonl"
    fi

    # Run log analysis
    echo ""
    echo "▶ Analyzing evaluation log..."
    "$PYTHON_BIN" "${EVAL_DIR}/analyze_log.py" 2>&1 | tee "${LOG_DIR}/analysis_report.txt" || true

    # Also copy the analysis report if generated in eval dir
    if [[ -f "${EVAL_DIR}/analysis_report.txt" ]]; then
        cp "${EVAL_DIR}/analysis_report.txt" "${LOG_DIR}/analysis_report.txt" 2>/dev/null || true
    fi

    echo ""
else
    echo "▶ Skipping Step 5 (--skip_eval)"
    echo ""
fi

# ============================================================
# Update run metadata with completion + scores
# ============================================================
"$PYTHON_BIN" -c "
import json, os
meta_path = '${RUN_DIR}/run_meta.json'
with open(meta_path) as f:
    meta = json.load(f)
meta['completed_at'] = '$(date -u +%Y-%m-%dT%H:%M:%SZ)'
log_path = '${LOG_DIR}/log_sqlpp.jsonl'
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
echo "  Run directory:  ${RUN_DIR}"
echo "  Submission:     ${SUBMISSION_DIR}"
echo "  Logs:           ${LOG_DIR}"
echo "=========================================="
