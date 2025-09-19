#!/bin/bash

# Default value if no argument is provided
suffix=''
# Check if an argument is provided
if [ "$#" -ge 1 ]; then
    suffix="$1"
fi

# Delete the dist folder if it exists
rm -rf dist/ .venv/

# Setup python and build the package
uv build --wheel --verbose

# Find the `.whl` file
folder_path="dist"
# Use find command to get paths of all .whl files within the folder
whl_files=$(find "$folder_path" -type f -name "*.whl")
# pip install
uv run pip install "$whl_files$suffix"
