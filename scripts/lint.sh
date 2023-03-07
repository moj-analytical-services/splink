#!/bin/bash

line_block="=============="
black_run="Running black to clean your files"
echo "$line_block $black_run $line_block"

black .

OPTIND=1

ruff_run="Running the ruff linter"
echo "$line_block $ruff_run $line_block"

while getopts ":f" opt; do
  case $opt in
    f)
      echo "--fix was run for your scripts" >&2
      ruff --fix . --quiet
      ;;
    \?)
      echo "Invalid option: -$OPTARG. Only -f (fix) is accepted." >&2
      ;;
  esac
done

ruff --show-source .
