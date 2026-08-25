#!/bin/bash
# Render all .nb.py notebooks found in the docs/ directory to executed .ipynb files
# using jupytext. Options:
#   -j N    maximum parallel jobs (default: all notebooks run in parallel)
#   [DIR]   subdirectory to limit rendering to (default: .)

JOBS=0
ROOT_DIR="."

while [[ $# -gt 0 ]]; do
  case "$1" in
    -j) JOBS="$2"; shift 2 ;;
    *)  ROOT_DIR="$1"; shift ;;
  esac
done

if [[ ! -d "$ROOT_DIR" ]]; then
  echo "Error: directory does not exist: $ROOT_DIR" >&2
  exit 1
fi

SCRIPT_START=$(date +%s)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

pids=()
files=()
i=0

run_notebook() {
  local f="$1" idx="$2"
  local out="${f%.nb.py}.ipynb"
  local nb_start nb_end elapsed

  nb_start=$(date +%s)
  if uv run jupytext --to notebook --execute --run-path . --set-kernel python3 --output "$out" "$f" > "$TMP/$idx.log" 2>&1; then
    nb_end=$(date +%s)
    elapsed=$(( nb_end - nb_start ))
    echo "success:$elapsed" > "$TMP/$idx.result"
  else
    nb_end=$(date +%s)
    elapsed=$(( nb_end - nb_start ))
    echo "fail:$elapsed" > "$TMP/$idx.result"
  fi
}

while IFS= read -r f; do
  echo "=== starting $f ==="
  files+=("$f")
  run_notebook "$f" "$i" &
  pids+=($!)

  # Throttle: wait for the oldest job before launching another
  if (( JOBS > 0 && ${#pids[@]} >= JOBS )); then
    wait "${pids[0]}"
    pids=("${pids[@]:1}")
  fi

  i=$(( i + 1 ))
done < <(find "$ROOT_DIR" -type f -name '*.nb.py')

for pid in "${pids[@]}"; do
  wait "$pid"
done

successfilesarray=()
failedfilesarray=()
notebook_times=()

for (( j=0; j<${#files[@]}; j++ )); do
  f="${files[$j]}"
  echo ""
  echo "=== output: $f ==="
  cat "$TMP/$j.log"

  if [[ -f "$TMP/$j.result" ]]; then
    content=$(< "$TMP/$j.result")
    status="${content%%:*}"
    elapsed="${content#*:}"
  else
    status="fail"
    elapsed="?"
  fi

  notebook_times+=("${elapsed}:${f}")
  if [[ "$status" == "success" ]]; then
    successfilesarray+=("$f")
    echo "OK: $f (${elapsed}s)"
  else
    failedfilesarray+=("$f")
    echo "FAIL: $f (${elapsed}s)"
  fi
done

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
