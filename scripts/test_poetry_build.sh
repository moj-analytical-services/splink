#!/bin/bash

# If the user requests that the current virtual
# environment be used - "--current_venv" -
# install poetry (if not already installed) and the latest Splink build.
# Otherwise, deactivate and create a new venv.
if [ "$1" == "--current_venv" ]; then
    pip3 install poetry
else
    deactivate
    # Delete the dist folder if it exists
    rm -rf dist/
    rm -rf venv

    # Setup python and build the package
    python3 -m venv venv
    source venv/bin/activate
fi

# Create the wheel for the poetry build
poetry build --format=wheel --verbose

# Find the `.whl` file
folder_path="dist"
# Use find command to get paths of all .whl files within the folder
whl_files=$(find "$folder_path" -type f -name "*.whl")
# pip install
pip3 install "$whl_files"
