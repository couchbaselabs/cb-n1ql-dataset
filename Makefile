# ============================================================
#  cb-n1ql-dataset — MCP Pipeline Makefile
#  All options are configured via .env — run `make setup` to create it.
#  Run `make help` to see available targets.
# ============================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help

PYTHON := $(shell command -v python3 2>/dev/null || command -v python)

# Load .env if present so PIPELINE_* vars are available here
ifneq (,$(wildcard .env))
  include .env
  export
endif

# Read pipeline options from .env with fallback defaults
DATASET := $(or $(PIPELINE_DATASET),sqlite)
TAG     := $(PIPELINE_TAG)
LIMIT   := $(or $(PIPELINE_LIMIT),0)
WORKERS := $(or $(PIPELINE_WORKERS),1)
TIMEOUT := $(or $(PIPELINE_TIMEOUT),360)

# Build run_mcp.sh arg string from env vars
RUN_ARGS := --mode "$(DATASET)"
ifneq ($(TAG),)
  RUN_ARGS += --tag "$(TAG)"
endif
ifneq ($(LIMIT),0)
  RUN_ARGS += --limit "$(LIMIT)"
endif
ifneq ($(WORKERS),1)
  RUN_ARGS += --max_workers "$(WORKERS)"
endif
ifneq ($(TIMEOUT),360)
  RUN_ARGS += --timeout "$(TIMEOUT)"
endif

# ============================================================
.PHONY: help setup run generate eval analyze quicktest clean

help:
	@$(PYTHON) -c "\
print(''' \
Usage: make <target> \
\nAll options are configured in config.json — run make setup to apply them to .env. \
\n\nTargets: \
\n  help         Show this help message \
\n  setup        Generate .env from values set in config.json \
\n  run          Run full MCP pipeline (generate -> evaluate -> analyze) \
\n  generate     Generate SQL++ queries only (skip evaluation) \
\n  eval         Evaluate an existing submission (skip generation) \
\n  analyze      Re-run analysis on an existing evaluation log \
\n  quicktest    Quick sanity check: run 1 question end-to-end \
\n  clean        Remove the run directory for PIPELINE_TAG \
\n\nSetup workflow: \
\n  1. Fill in value fields in config.json \
\n  2. Run make setup to generate .env \
'''); \
"
	@echo ""
	@echo "Current settings (from .env):"
	@echo "  PIPELINE_DATASET    = $(DATASET)"
	@echo "  PIPELINE_TAG     = $(TAG)"
	@echo "  PIPELINE_LIMIT   = $(LIMIT)"
	@echo "  PIPELINE_WORKERS = $(WORKERS)"
	@echo "  PIPELINE_TIMEOUT = $(TIMEOUT)"
	@echo ""

setup:
	@$(PYTHON) scripts/setup.py

run:
	@./run_mcp.sh $(RUN_ARGS)

generate:
	@./run_mcp.sh $(RUN_ARGS) --skip_eval

eval:
	@./run_mcp.sh $(RUN_ARGS) --eval_only

analyze:
	@if [ -z "$(TAG)" ]; then RUN_DIR="runs/mcp"; else RUN_DIR="runs/mcp_$(TAG)"; fi; \
	LOG="$$RUN_DIR/logs/log_sqlpp_catalog.jsonl"; \
	if [ ! -f "$$LOG" ]; then echo "Error: log not found at $$LOG"; exit 1; fi; \
	cp "$$LOG" evaluation_pipeline/log_sqlpp_catalog.jsonl; \
	$(PYTHON) evaluation_pipeline/analyze_log.py 2>&1 | tee "$$RUN_DIR/logs/analysis_report.txt"

quicktest:
	@echo "Running quick test (1 question, dataset=$(DATASET))..."
	@./run_mcp.sh --mode "$(DATASET)" --tag quicktest --limit 1
	@echo ""
	@echo "Quick test complete. Check runs/mcp_quicktest/ for results."

clean:
	@if [ -z "$(TAG)" ]; then \
		echo "Error: PIPELINE_TAG is not set in .env"; exit 1; \
	fi; \
	RUN_DIR="runs/mcp_$(TAG)"; \
	if [ ! -d "$$RUN_DIR" ]; then echo "Error: not found: $$RUN_DIR"; exit 1; fi; \
	echo "Removing $$RUN_DIR ..."; rm -rf "$$RUN_DIR"; echo "Done."
