# TODO: encapsulate different uv run / sync configurations

# TODO: add docs, once custom config sorted?
lint:
	uv run ruff check splink tests

format:
	uv run ruff format splink tests && uv run ruff check --fix splink tests

format-check:
	uv run ruff format --check splink tests

typecheck:
	uv run mypy splink

typecheck-dev:
	uv run ty check splink

check: format-check lint typecheck

test:
	uv run python -m pytest -vm "duckdb and not needs_pandas" tests/

pg-start:
	./scripts/postgres_docker/setup.sh

pg-stop:
	./scripts/postgres_docker/teardown.sh
