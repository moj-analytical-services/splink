#!/bin/bash

line_block="=============="
black_run="Running black to clean your files"
echo "$line_block $black_run $line_block"

black .

ruff_run="Running the ruff linter"
echo "$line_block $ruff_run $line_block"

OPTIND=1

while getopts ":f" opt; do
  case $opt in
    f)
      ruff --fix splink/ --quiet
      ruff --fix tests/ --quiet
      ruff --fix scripts/ --quiet
      echo "--fix was run for your scripts" >&2
      ;;
    \?)
      echo "Invalid option: -$OPTARG. Only -f (fix) is accepted." >&2
      ;;
  esac
done

ruff --show-source splink/
ruff --show-source tests/
ruff --show-source scripts/
