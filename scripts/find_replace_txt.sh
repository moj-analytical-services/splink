#!/bin/bash

original_txt=(
    '=== "Spark"'
    '=== "DuckDB"'
    '=== "Athena"'
    '=== "PostgreSql"'
    '=== "SQLite"'
)
replacement_txt=(
    '=== ":simple-apachespark: Spark"'
    '=== ":simple-duckdb: DuckDB"'
    '=== ":simple-amazonaws: Athena"'
    '=== ":simple-postgresql: PostgreSql"'
    '=== ":simple-sqlite: SQLite"'
)

# # Check if the lengths of the arrays are equal
if [ ${#original_txt[@]} -eq ${#replacement_txt[@]} ]; then
    # Initialize the command variable
    command=()

    # Loop through the arrays simultaneously and create the command
    for ((i=1; i<=${#original_txt[@]}; i++)); do
        c="-e "  # only way I could get it to register `-e` without failure
        c+="s/${original_txt[i]}/${replacement_txt[i]}/g"
        command+=$c
    done

    # appends a series of `-e {find/replace}` calls
    find . -type f \( -name "*.py" -o -name "*.md" \) -exec sed -i '' $command {} +

else
    echo "Error: Array lengths do not match"
fi
