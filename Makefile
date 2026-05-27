# ============================================================
#  cb-n1ql-dataset — MCP Pipeline Makefile
#  All options are configured via config.json — run `make setup` to apply.
#  Run `make help` to see available targets.
# ============================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help

PYTHON := $(shell command -v python3 2>/dev/null || command -v python)

# Load .env if present
ifneq (,$(wildcard .env))
  include .env
  export
endif

DATASET := $(or $(PIPELINE_DATASET),sqlite)
TAG     := $(PIPELINE_TAG)
LIMIT   := $(or $(PIPELINE_LIMIT),0)
WORKERS := $(or $(PIPELINE_WORKERS),1)
TIMEOUT := $(or $(PIPELINE_TIMEOUT),360)

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
.PHONY: help setup run quicktest

help:
	@$(PYTHON) -c "\
print(''' \
Usage: make <target> \
\nAll options are configured in config.json — run make setup to apply them to .env. \
\n\nTargets: \
\n  help       Show this help message \
\n  setup      Generate .env from values set in config.json \
\n  run        Run full MCP pipeline (generate -> evaluate -> analyze) \
\n  quicktest  Quick sanity check: run 1 question end-to-end \
\n\nSetup workflow: \
\n  1. Fill in value fields in config.json \
\n  2. Run make setup to generate .env \
\n  3. Run make quicktest to verify the pipeline works \
\n  4. Run make run to run the full pipeline \
'''); \
"
	@echo "Current settings (from .env):"
	@echo "  PIPELINE_DATASET = $(DATASET)"
	@echo "  PIPELINE_TAG     = $(TAG)"
	@echo "  PIPELINE_LIMIT   = $(LIMIT)"
	@echo "  PIPELINE_WORKERS = $(WORKERS)"
	@echo "  PIPELINE_TIMEOUT = $(TIMEOUT)"
	@echo ""

setup:
	@$(PYTHON) scripts/setup.py

run:
	@./run_mcp.sh $(RUN_ARGS)

quicktest:
	@echo "Running quick test (1 question, dataset=$(DATASET))..."
	@./run_mcp.sh --mode "$(DATASET)" --tag quicktest --limit 1
