.PHONY: help install test lint fmt fmt-check typecheck audit build check run-brain run-executor run-archiver provision clean

help:
	@echo "Mercury — common development tasks"
	@echo ""
	@echo "  make install       Sync dependencies via uv (runtime + dev)"
	@echo "  make test          Run pytest"
	@echo "  make lint          Ruff lint check"
	@echo "  make fmt           Ruff format (writes changes)"
	@echo "  make fmt-check     Ruff format check (no writes)"
	@echo "  make typecheck     Mypy on all runtime packages"
	@echo "  make audit         Audit the active environment for known vulnerabilities"
	@echo "  make build         Build source and wheel distributions"
	@echo "  make check         lint + format + types + tests + audit + build"
	@echo ""
	@echo "  make run-brain     Start the brain service"
	@echo "  make run-executor  Start the executor service"
	@echo "  make run-archiver  Start the Plutarch archiver"
	@echo "  make provision     Register Nephtys streams"
	@echo "  make clean         Remove caches"

install:
	uv sync

test:
	uv run pytest

lint:
	uv run ruff check .

fmt:
	uv run ruff format .

fmt-check:
	uv run ruff format --check .

typecheck:
	uv run mypy core brain executor plutarch backtesting

audit:
	uv run pip-audit

build:
	uv build

check: lint fmt-check typecheck test audit build

run-brain:
	uv run python -m brain.main

run-executor:
	uv run python -m executor.main

run-archiver:
	uv run python -m plutarch.archiver

provision:
	uv run python scripts/provision_nephtys.py

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
