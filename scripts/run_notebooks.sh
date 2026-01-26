#!/bin/bash
# Run all .nb.py notebooks found in the docs/ directory
# Optionally pass a subdirectory as command line argument to limit to that subdirectory

successfilesarray=()
failedfilesarray=()

ROOT_DIR="docs"
if [[ -n "$1" ]]; then
  ROOT_DIR="$ROOT_DIR/$1"
fi

if [[ ! -d "$ROOT_DIR" ]]; then
  echo "Error: directory does not exist: $ROOT_DIR" >&2
  exit 1
fi

while IFS= read -r f; do
  echo "=== running $f ==="
  if uv run python "$f"; then
    successfilesarray+=("$f")
    echo "OK: $f (success count now ${#successfilesarray[@]})"
  else
    failedfilesarray+=("$f")
    echo "FAIL: $f (fail count now ${#failedfilesarray[@]})"
  fi
done < <(find "$ROOT_DIR" -type f -name '*.nb.py')

declare -p successfilesarray failedfilesarray
echo "=== Summary ==="
echo "Succeeded files:"
for file in "${successfilesarray[@]}"; do
  echo "$file"
done
echo ""
echo "Failed files:"
for file in "${failedfilesarray[@]}"; do
  echo "$file"
done
