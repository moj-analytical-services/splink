#!/bin/bash

deactivate
# Delete the dist folder if it exists
rm -rf dist/
rm -rf venv

# Setup python and build the package
python3 -m venv venv
source venv/bin/activate
poetry build --format=wheel --verbose

# Find the `.whl` file
folder_path="dist"
# Use find command to get paths of all .whl files within the folder
whl_files=$(find "$folder_path" -type f -name "*.whl")
# pip install
# pip3 install "$whl_files"
pip3 install $whl_files'[spark]'