#!/bin/bash

line_block="=============="

package_name="aspell"
pyspelling_yaml="scripts/pyspelling/pyspelling.yml"
default_path_to_spellcheck="docs"

# Use either the first command line arg or the default path to spellcheck
path_to_spellcheck="${1:-$default_path_to_spellcheck}"
echo "Path to spellcheck: $path_to_spellcheck"

# Function to check if necessary packages are installed
source scripts/utils/ensure_packages_installed.sh
ensure_homebrew_packages_installed aspell yq

cwd=$(pwd)

# Set up venv, install pyspelling and download dictionary files
if [[ "$VIRTUAL_ENV" != "$cwd/spellcheck-venv" ]]; then
    # If already in a venv then deactivate
    if [ -n "$VIRTUAL_ENV" ]; then
        deactivate
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


# Finally, validate the path or file that the user has entered to be spellchecked
if [ -d "$path_to_spellcheck" ]; then
    # Checks if a directory has been entered and adds a recursive search for markdown files
    source_to_spellcheck="$path_to_spellcheck"/**/*.md
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

pyspelling_run="Running pyspelling spellchecker on docs"
echo "$line_block $pyspelling_run $line_block"

# Update pyspelling.yml with a new source path
yq e ".matrix[0].sources = [\"$source_to_spellcheck|!docs/includes/**/*.md\"]" -i "$pyspelling_yaml"

echo $source_to_spellcheck
pyspelling -c ./$pyspelling_yaml
