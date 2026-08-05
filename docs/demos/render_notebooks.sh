#!/bin/bash
# Render all .nb.py notebooks found in the docs/ directory to executed .ipynb files
# using jupytext. Optionally pass a subdirectory as command line argument to limit
# to that subdirectory.

successfilesarray=()
failedfilesarray=()
notebook_times=()  # entries are "elapsed:filepath"

SCRIPT_START=$(date +%s)

ROOT_DIR="."
if [[ -n "$1" ]]; then
  ROOT_DIR="$1"
fi

if [[ ! -d "$ROOT_DIR" ]]; then
  echo "Error: directory does not exist: $ROOT_DIR" >&2
  exit 1
fi

while IFS= read -r f; do
  echo "=== rendering $f ==="
  nb_start=$(date +%s)
  out="${f%.nb.py}.ipynb"
  if uv run jupytext --to notebook --execute --run-path . --set-kernel python3 --output "$out" "$f"; then
    nb_end=$(date +%s)
    elapsed=$(( nb_end - nb_start ))
    notebook_times+=("${elapsed}:${f}")
    successfilesarray+=("$f")
    echo "OK: $f (${elapsed}s) (success count now ${#successfilesarray[@]})"
  else
    nb_end=$(date +%s)
    elapsed=$(( nb_end - nb_start ))
    notebook_times+=("${elapsed}:${f}")
    failedfilesarray+=("$f")
    echo "FAIL: $f (${elapsed}s) (fail count now ${#failedfilesarray[@]})"
  fi
done < <(find "$ROOT_DIR" -type f -name '*.nb.py')

SCRIPT_END=$(date +%s)
TOTAL=$(( SCRIPT_END - SCRIPT_START ))

echo ""
echo "=== Notebook timings ==="
for entry in "${notebook_times[@]}"; do
  elapsed="${entry%%:*}"
  file="${entry#*:}"
  echo "  ${elapsed}s  $file"
done

echo ""
echo "=== Summary ==="
echo "Succeeded files:"
for file in "${successfilesarray[@]}"; do
  echo "  $file"
done
echo ""
echo "Failed files:"
for file in "${failedfilesarray[@]}"; do
  echo "  $file"
done

echo ""
echo "Total time: ${TOTAL}s"

if (( ${#failedfilesarray[@]} > 0 )); then
  exit 1
fi

exit 0
