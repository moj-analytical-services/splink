#!/bin/bash

cd "$(dirname "$0")"
cd ..

set -e

if [[ ! -d "docs-venv" ]]; then
    python3 -m venv docs-venv
    source docs-venv/bin/activate
    pip install --upgrade pip
    pip install -r scripts/docs-requirements.txt
else
    source docs-venv/bin/activate
fi

# Need to update generated content separately using main Splink env
if [[ ! -f "docs/includes/generated_files/comparison_level_library_dialect_table.md" ]]
then
    if [ -n "$VIRTUAL_ENV" ]; then
        deactivate
    fi
    poetry run python3 scripts/generate_dataset_docs.py
fi

if [[ "$VIRTUAL_ENV" != "$(pwd)/docs-venv" ]]; then
    source docs-venv/bin/activate
fi
# can remove verbose flag if needed
mkdocs serve -v --dirtyreload
