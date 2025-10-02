#!/bin/bash

cd "$(dirname "$0")"
cd ..

set -e

# Need to update generated content separately using main Splink env
if [[ ! -f "docs/includes/generated_files/datasets_table.md" ]]
then
    uv run python scripts/generate_dataset_docs.py
fi

# can remove verbose flag if needed
uv run --group docs python -m mkdocs serve -v --dirtyreload
