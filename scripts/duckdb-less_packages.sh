#!/bin/bash

# Set params
requirements_file="requirements.txt"
packages=("duckdb" "numpy")

rm -rf "$requirements_file"
poetry export --without-hashes --only main  -f "$requirements_file" -o "$requirements_file"

# Create a temporary file to store the modified content
temp_file=$(mktemp)

# Delete the line containing the package from the requirements file
grep -vE "^($(IFS="|"; echo "${packages[*]}"))==" "$requirements_file" > "$temp_file"

# Overwrite the original requirements file with the modified content
mv "$temp_file" scripts/duckdbless_requirements.txt
rm -rf "$requirements_file"
