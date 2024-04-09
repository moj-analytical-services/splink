#!/bin/bash

cd "$(dirname "$0")"

set -e

line_block="=============="

# Use either the first command line arg or the default path to spellcheck
path_to_spellcheck="${1:-docs}"
echo "Path to spellcheck: $path_to_spellcheck"

source ../utils/ensure_packages_installed.sh
if ! command -v aspell &> /dev/null
then
    ensure_homebrew_packages_installed aspell
fi

# Go up to the root of the repo
cd ../..

# Set up venv, install pyspelling and download dictionary files
if [[ "$VIRTUAL_ENV" != "$(pwd)/spellcheck-venv" ]]; then
    # If already in a venv then deactivate
    if [ -n "$VIRTUAL_ENV" ]; then
        deactivate
    fi

    if ! [ -d spellcheck-venv ]; then
        echo "$line_block Creating spellchecking venv $line_block"
        python3 -m venv spellcheck-venv
        source spellcheck-venv/bin/activate
        echo "$line_block Installing pyspelling $line_block"
        python -m pip install pyspelling
    else
        source spellcheck-venv/bin/activate
    fi
fi

# Finally, validate the path or file that the user has entered to be spellchecked
if [ -d "$path_to_spellcheck" ]; then
    # Checks if a directory has been entered and adds a recursive search for markdown files
    source_to_spellcheck="$path_to_spellcheck"'/**/*.md'
elif [ -f "$path_to_spellcheck" ]; then
    # Checks that the file extension is .md
    if [[ $path_to_spellcheck == *.md ]]; then
        source_to_spellcheck="$path_to_spellcheck"
    else
        echo "The file is not a markdown file."
    return 0 2>/dev/null
    fi
else
    # Errors if an invalid input is entered
    echo -n "Invalid input. Please enter a valid directory, markdown file path, or "
    echo "use the default entrypoint (no argument) - 'docs'."
    return 0 2>/dev/null
fi

echo "$line_block Running pyspelling spellchecker on docs $line_block"

echo "$source_to_spellcheck"
pyspelling \
    -c ./scripts/pyspelling/pyspelling.yml \
    -n "Markdown docs" \
    -S "$source_to_spellcheck"'|!docs/includes/**/*.md'
