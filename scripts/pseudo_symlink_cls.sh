#!/bin/bash

# To do:
# grep + replace all imports in every script
linkers=(spark duckdb athena sqlite postgres)

deprecation_warning() {
  local pseudo_sym="$1"
  local cleaned_original_path="$2"
  local full_import_path="$3"

  echo "import warnings" >> "$pseudo_sym"
  echo "from ..exceptions import SplinkDeprecated" >> "$pseudo_sym"
  echo "warnings.warn('Importing directly from \`$cleaned_original_path\` '" >> "$pseudo_sym"
  echo "'is deprecated and will be removed in Splink v4. '" >> "$pseudo_sym"
  echo "'Please import from \`$full_import_path\` going forward.'," >> "$pseudo_sym"
  echo "SplinkDeprecated, stacklevel=2)" >> "$pseudo_sym"

  # Auto-format our script
  black $pseudo_sym
  ruff --fix $pseudo_sym
}


for backend in "${linkers[@]}"
do
    folder="splink/$backend"
    comparisons="${backend}_comparisons"

    ##############################
    ### LINKER PSEUDO SYMLINKS ###
    ##############################
    # pseudo link `linker.py` -> `{backend}_linker.py`
    linker_file="$folder"/linker.py
    # if it doesn't exist, rename the existing linker file
    original_linker_file="$folder/${backend}_${linker_file#"$folder"/}"
    if [ ! -e "$linker_file" ]; then
        mv "$original_linker_file" "$linker_file"
    fi

    # Extract Python class names using grep - this regex assumes classes are
    # standalone and defined in the typical standard:
    # class <name>():
    class_names=$(grep -Eo "^\s*class\s+[A-Za-z_][A-Za-z0-9_]*" "$linker_file" \
    | awk '{print $2}' \
    | paste -sd ", " -)

    cleaned_original_path=splink."$backend"."$backend"_linker
    full_import_path="splink.$backend.linker"

    # Create file and add new import path
    echo "from .linker import ($class_names)  # noqa: F401" > $original_linker_file
    # Write deprecation warning
    deprecation_warning $original_linker_file $cleaned_original_path $full_import_path

    ##################################
    ### COMPARISON PSEUDO SYMLINKS ###
    ##################################
    # Declare a list of comparison files that have already been evaluated
    declare -a comparison_files=()

    # Iterate over the files starting with "$backend_comparisons" in the folder
    for file in "${folder}"/*comparison*; do

        # Extract the filename without the backend extension
        # This is then used to check if the file has already been processed
        file_name=$(basename "$file")
        f_name=${file_name/"$backend"_/}

        if [[ " ${comparison_files[@]} " =~ " ${f_name} " ]]; then
            continue  # exit if we've already processed
        else
            comparison_files+=$f_name
        fi

        # A standardised file name that removes the backend. i.e.
        # duckdb_comparison_library.py -> comparison_library.py
        standardised_file="$folder/$f_name"

        # Generate our symlink name. This simply inserts `$backend`
        # into our existing the filepath.
        symlink_name="${folder}/${backend}_${standardised_file#"$folder"/}"
        import_name=$(basename "$standardised_file"); import_name="${import_name%.*}"

        # If standardised_file doesn't exist, simply replace it w/ `{backend}_comparison...`
        if [ ! -e "$standardised_file" ]; then
            mv "$symlink_name" "$standardised_file"
        fi

        # cleaned_original_path="splink."$backend"."$backend_""$import_name""
        cleaned_original_path="splink.${backend}.${backend}_${import_name}"
        full_import_path="splink.$backend.$import_name"

        # Create file and add new import path
        echo "from .$import_name import *  # noqa: F403" > $symlink_name
        # Write deprecation warning
        deprecation_warning $symlink_name $cleaned_original_path $full_import_path

    done
done
