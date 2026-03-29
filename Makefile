.PHONY: setup db-up db-down collect pipeline export lint test

setup:
	pip install -e ".[dev]"

db-up:
	docker compose up -d postgres

db-down:
	docker compose down

collect:
	python scripts/collect_all.py

pipeline:
	python scripts/run_pipeline.py

export:
	python scripts/export_catalog.py

lint:
	ruff check scripts tests

test:
	pytest -q
