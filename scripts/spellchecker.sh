#!/bin/bash

cwd=$(pwd)

if [[ "$VIRTUAL_ENV" != "$cwd/spellcheck-venv" ]]
then
    deactivate
    python3 -m venv spellcheck-venv                                                                 
    source spellcheck-venv/bin/activate
    python -m pip install pyspelling
    # need to install aspell
fi

line_block="=============="
pyspelling_run="Running pyspelling spellchecker on docs"
echo "$line_block $pyspelling_run $line_block"

pyspelling