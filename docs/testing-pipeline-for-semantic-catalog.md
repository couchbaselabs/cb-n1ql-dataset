# Testing Pipeline for Semantic Catalog

End-to-end evaluation pipeline for the Couchbase MCP server — generates SQL++ queries via the MCP tool and evaluates them against a live Couchbase cluster.

## How it works

```
config.json → .env → run_mcp.sh
                         │
                         ├── 1. Start iQ-FastAPI (query generation backend)
                         ├── 2. Run MCP client → generate SQL++ for each question
                         ├── 3. Copy .sqlpp files to submission/
                         └── 4. Evaluate against Couchbase + analyze results
```

Each run is saved under `runs/<IST_timestamp>[_tag]/`:
```
runs/20250519_143022_myrun/
├── run_meta.json          — timestamps, final score
├── submission/            — generated .sqlpp files
└── logs/
    ├── mcp_server.log     — MCP server stdout/stderr
    ├── iq_fastapi.log     — iQ-FastAPI stdout/stderr
    ├── log_sqlpp_catalog.jsonl — structured evaluation log
    └── analysis_report.txt
```

## Setup

### 1. Fill in config.json

Copy `config_example.json` to `config.json` and fill in your values:

```bash
cp config_example.json config.json
# edit config.json with your credentials and paths
```

Key fields to set:

| Key | What it is |
|-----|-----------|
| `IQ_FASTAPI_PATH` | Absolute path to the `iQ-FastAPI` directory |
| `IQ_FASTAPI_VENV_PATH` | Absolute path to iQ-FastAPI's Python venv |
| `MCP_SERVER_PATH` | Absolute path to `mcp-server-couchbase` |
| `MCP_SERVER_VENV_PATH` | Absolute path to the MCP server's Python venv |
| `IQ_OPENAI_API_KEY` | OpenAI API key (used by iQ-FastAPI) |
| `IQ_COUCHBASE_CONNECTION_STRING` | Couchbase cluster for iQ-FastAPI RAG |
| `MCP_CB_CONNECTION_STRING` | Couchbase cluster the MCP server queries |
| `COUCHBASE_HOST` | Couchbase cluster for the evaluator |

### 2. Generate .env

```bash
make setup
```

This reads all `value` fields from `config.json` and writes them to `.env`. Required fields with empty values will trigger a warning.

### 3. Verify the pipeline

```bash
make quicktest
```

Runs a single question end-to-end. Check `runs/<latest>/logs/` if anything fails.

### 4. Run the full pipeline

```bash
make run
```

## Configuration

All settings live in `config.json`. Run `make setup` after any change to regenerate `.env`.

| Variable | Default | Description |
|----------|---------|-------------|
| `PIPELINE_DATASET` | `sqlite` | Question set: `sqlite`, `snowflake`, or `bird` |
| `PIPELINE_TAG` | _(none)_ | Suffix appended to the run folder name |
| `PIPELINE_LIMIT` | `0` (all) | Cap the number of questions processed |
| `PIPELINE_WORKERS` | `1` | Parallel workers for evaluation |
| `PIPELINE_TIMEOUT` | `360` | Per-question timeout in seconds |

## Running specific datasets

The dataset maps to a questions file under `test/`:

| `PIPELINE_DATASET` | File |
|--------------------|------|
| `sqlite` | `test/questions_sqlite.json` |
| `snowflake` | `test/questions_snowflake.json` |
| `bird` | `test/questions_bird.json` |

Set `PIPELINE_DATASET` in `config.json`, then `make setup && make run`.

## Advanced: skip generation or evaluation

Pass flags directly to `run_mcp.sh`:

```bash
# Generate only, skip evaluation
./run_mcp.sh --skip_eval

# Evaluate an existing submission (skip generation)
./run_mcp.sh --eval_only
```

## Environment variable conventions

- `IQ_*` vars are passed to iQ-FastAPI with the `IQ_` prefix stripped.
- `MCP_CB_*` vars are passed to the MCP server as `CB_*` (prefix replaced).
- `CB_MCP_ENABLE_QUERY_GENERATION=True` must use capital `T` (click bool parsing).
