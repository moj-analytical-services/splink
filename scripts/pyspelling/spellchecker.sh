#!/bin/bash

cwd=$(pwd)

if [[ "$VIRTUAL_ENV" != "$cwd/spellcheck-venv" ]]
then
    deactivate
    python3 -m venv spellcheck-venv                                                                 
    source spellcheck-venv/bin/activate
    python -m pip install pyspelling
    # Install aspell
    brew install aspell
    # Donwload dictionary files into correct directory
    curl -LJ https://github.com/LibreOffice/dictionaries/raw/master/en/en_GB.dic -o ~/Library/Spelling/en_GB.dic
    curl -LJ https://github.com/LibreOffice/dictionaries/blob/master/en/en_GB.aff -o ~/Library/Spelling/en_GB.aff

fi

line_block="=============="
pyspelling_run="Running pyspelling spellchecker on docs"
echo "$line_block $pyspelling_run $line_block"

pyspelling -c ./scripts/pyspelling/.pyspelling.yml