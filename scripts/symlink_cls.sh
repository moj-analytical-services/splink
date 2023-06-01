#!/bin/bash

linkers=(spark duckdb athena sqlite)

for backend in "${linkers[@]}"
do
    folder="splink/$backend"
    comparisons="${backend}_comparisons"


    # Iterate over the files starting with "$backend_comparisons" in the folder
    # for file in "$folder/$comparisons"*; do
    for file in "${folder}/${backend}_comparison"*; do

        # Extract the filename without the "$backend_" prefix
        symlink_name="$folder/${file#"$folder"/${backend}_}"

        if [ ! -e "$symlink_name" ]; then
            ln -s "../../$file" $symlink_name
        fi
    done
done
