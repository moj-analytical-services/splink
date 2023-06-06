#!/bin/bash

linkers=(spark duckdb athena sqlite)

for backend in "${linkers[@]}"
do
    folder="splink/$backend"
    cd $folder
    comparisons="${backend}_comparisons"

    # Iterate over the files starting with "$backend_comparisons" in the folder
    for file in "comparison_"*; do

        # Generate our symlink name. This simply insert `$backend`
        # into our existing the filepath.
        symlink_name="${backend}_${file}"
        rm $symlink_name

        ln -s $file "./$symlink_name"
    done

    # Exit the folder
    cd ../..

done
