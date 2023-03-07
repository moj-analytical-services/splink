#!/bin/bash

echo "======================="
echo "Running black to clean your files"
echo "======================="
black .

OPTIND=1

echo "======================="
echo "Running the ruff linter"
echo "======================="

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
