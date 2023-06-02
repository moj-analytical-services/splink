#!/bin/bash

linkers=(spark duckdb athena sqlite)

for backend in "${linkers[@]}"
do
    folder="splink/$backend"
    comparisons="${backend}_comparisons"


    # Iterate over the files starting with "$backend_comparisons" in the folder
    # for file in "${folder}/${backend}_comparison"*; do
    for file in "${folder}/comparison_"*; do

        # Generate our symlink name. This simply insert `$backend`
        # into our existing the filepath.
        symlink_name="${folder}/${backend}_${file#"$folder"/}"

        if [ ! -e "$symlink_name" ]; then
            ln -s "../../$file" $symlink_name
        fi
    done
done
