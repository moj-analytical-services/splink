#!/bin/bash

successfilesarray=()
failedfilesarray=()

while IFS= read -r f; do
  echo "=== running $f ==="
  if uv run python "$f"; then
    successfilesarray+=("$f")
    echo "OK: $f (success count now ${#successfilesarray[@]})"
  else
    failedfilesarray+=("$f")
    echo "FAIL: $f (fail count now ${#failedfilesarray[@]})"
  fi
done < <(find docs -type f -name '*.nb.py')

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
