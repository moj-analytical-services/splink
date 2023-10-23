#!/bin/bash

# Default value if no argument is provided
suffix=''
# Check if an argument is provided
if [ "$#" -ge 1 ]; then
    suffix="$1"
fi

# Deactivate the current virtual environment if it is active
if command -v deactivate > /dev/null 2>&1; then
    deactivate
fi
# Delete the dist folder if it exists
rm -rf dist/ venv/

# Setup python and build the package
python3 -m venv venv && source venv/bin/activate
pip install --upgrade pip
pip install poetry && poetry build --format=wheel --verbose

# Find the `.whl` file
folder_path="dist"
# Use find command to get paths of all .whl files within the folder
whl_files=$(find "$folder_path" -type f -name "*.whl")
# pip install
pip3 install "$whl_files$suffix"