#!/bin/bash

line_block="=============="

package_name="aspell"

#Check if Homebrew is installed
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
    echo "Homebrew is not installed on this system."
    echo "Please install via https://brew.sh/ and rerun spellchecker.sh"
    return
fi

cwd=$(pwd)

# Set up venv, install pyspelling and download dictionary files
if [[ "$VIRTUAL_ENV" != "$cwd/spellcheck-venv" ]]; then
    # If already in a venv then deactivate
    if [ -n "$VIRTUAL_ENV" ]; then
        deactivate
    else    
        :
    fi
    # Set up venv
    python3 -m venv spellcheck-venv                                                                 
    source spellcheck-venv/bin/activate
    # Install pyspelling
    echo "$line_block Installing pyspelling $line_block"
    python -m pip install pyspelling
    # Download dictionary files into correct directory
    echo "$line_block Downloading dictionay files to Library/Spelling $line_block"
    curl -LJ https://github.com/LibreOffice/dictionaries/raw/master/en/en_GB.dic -o ~/Library/Spelling/en_GB.dic
    curl -LJ https://github.com/LibreOffice/dictionaries/blob/master/en/en_GB.aff -o ~/Library/Spelling/en_GB.aff
fi

pyspelling_run="Running pyspelling spellchecker on docs"
echo "$line_block $pyspelling_run $line_block"

pyspelling -c ./scripts/pyspelling/.pyspelling.yml