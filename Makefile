PYTHON ?= .venv/bin/python
PROTO  := dynamo/proto/dynamo.proto

.PHONY: help install proto lint test test-unit test-integration test-e2e test-report cluster stop demo experiments plots clean

help:            ## Show this help
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  %-18s %s\n", $$1, $$2}'

install:         ## Create .venv and install the package with dev + bench extras
	python3 -m venv .venv
	$(PYTHON) -m pip install -U pip
	$(PYTHON) -m pip install -e '.[dev,bench]'

proto:           ## Regenerate gRPC/protobuf bindings from dynamo.proto
	$(PYTHON) -m grpc_tools.protoc -I . --python_out=. --pyi_out=. --grpc_python_out=. $(PROTO)

lint:            ## Run ruff
	$(PYTHON) -m ruff check dynamo tests benchmarks

test: lint       ## Lint + full test suite
	$(PYTHON) -m pytest

test-unit:       ## Unit tests only (fast)
	$(PYTHON) -m pytest tests/unit

test-integration: ## In-process cluster tests
	$(PYTHON) -m pytest tests/integration

test-e2e:        ## Real multi-process cluster tests
	$(PYTHON) -m pytest tests/e2e

test-report:     ## Run all tests and write docs/TEST_REPORT.md
	$(PYTHON) scripts/test_report.py

cluster:         ## Start a local 3-node cluster in the background
	./scripts/start_cluster.sh 3

stop:            ## Stop the local cluster
	./scripts/stop_cluster.sh

demo:            ## Guided walkthrough on a live 4-node cluster
	./scripts/demo.sh

experiments:     ## Re-run every experiment and redraw the charts (~25 min)
	$(PYTHON) -m benchmarks.run_experiments all

plots:           ## Redraw the experiment charts from existing results
	$(PYTHON) -m benchmarks.plots

clean:           ## Remove runtime state and caches
	rm -rf data logs pids bench_out .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
