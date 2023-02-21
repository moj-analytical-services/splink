#!/bin/bash
UPDATE="TRUE"

if [[ $UPDATE == "TRUE" ]]
then
    rm -rf demos/
    git clone https://github.com/moj-analytical-services/splink_demos.git demos/
fi

deactivate
python3 -m venv docs-venv
source docs-venv/bin/activate
pip install --upgrade pip
pip install -r scripts/docs-requirements.txt

mkdocs serve
