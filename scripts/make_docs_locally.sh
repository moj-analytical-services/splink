#!/bin/bash

# Need to update generated content separately using poetry env:
# python scripts/json_schema_to_md_doc.py
# python scripts/generate_dialect_comparison_docs.py

# and preprocess include files with python scripts/preprocess.py
# as include-markdown plugin clashes with mknotebooks
# make sure not to commit changes to files with inclusions

UPDATE="TRUE"
if [[ $UPDATE == "TRUE" ]]
then
    rm -rf docs/demos/
    git clone https://github.com/moj-analytical-services/splink_demos.git docs/demos/
fi

deactivate
python3 -m venv docs-venv
source docs-venv/bin/activate
pip install --upgrade pip
pip install -r scripts/docs-requirements.txt

# can remove verbose flag if needed
mkdocs serve -v
