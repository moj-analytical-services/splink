#!/bin/bash

line_block="=============="

package_name="aspell"

# Check if Homebrew is installed
if command -v brew >/dev/null 2>&1; then
    # Check if aspell is installed
    if brew list --formula | grep -q "${package_name}"; then
        :
    else
        echo "Spellchecker package ${package_name} is not installed."
        echo "$line_block Installing ${package_name} $line_block"
        brew install aspell
    fi
else
    echo "Homebrew is not installed on this system. Please install via https://brew.sh/"
    # exit 1
fi

cwd=$(pwd)

if [[ "$VIRTUAL_ENV" != "$cwd/spellcheck-venv" ]]
then
    deactivate
    python3 -m venv spellcheck-venv                                                                 
    source spellcheck-venv/bin/activate
    python -m pip install pyspelling
    # Donwload dictionary files into correct directory

    curl -LJ https://github.com/LibreOffice/dictionaries/raw/master/en/en_GB.dic -o ~/Library/Spelling/en_GB.dic
    curl -LJ https://github.com/LibreOffice/dictionaries/blob/master/en/en_GB.aff -o ~/Library/Spelling/en_GB.aff
fi

pyspelling_run="Running pyspelling spellchecker on docs"
echo "$line_block $pyspelling_run $line_block"

pyspelling -c ./scripts/pyspelling/.pyspelling.yml