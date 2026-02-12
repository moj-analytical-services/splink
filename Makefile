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

check: format-check lint typecheck

