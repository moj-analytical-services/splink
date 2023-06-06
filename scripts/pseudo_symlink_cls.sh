#!/bin/bash

linkers=(spark duckdb athena sqlite)

for backend in "${linkers[@]}"
do
    folder="splink/$backend"
    comparisons="${backend}_comparisons"


    # Iterate over the files starting with "$backend_comparisons" in the folder
    for file in "${folder}/comparison_"*; do

        # Generate our symlink name. This simply insert `$backend`
        # into our existing the filepath.
        symlink_name="${folder}/${backend}_${file#"$folder"/}"
        import_name=$(basename "$file"); import_name="${import_name%.*}"

        if [ ! -e "$symlink_name" ]; then
            rm $symlink_name
        fi

        # Write import paths to our files
        echo "from .$import_name import *" > $symlink_name

    done
done
