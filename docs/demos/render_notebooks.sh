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

# Runs jupytext with a 300s timeout (enough headroom above any legitimate notebook runtime)
# to ensure ZMQ kernel-connection hangs are detected rather than blocking indefinitely.
_jupytext_exec() {
  local f="$1" logfile="$2" out="${1%.nb.py}.ipynb"
  if command -v timeout &>/dev/null; then
    timeout 300 uv run jupytext --to notebook --execute --run-path . --set-kernel python3 --output "$out" "$f" >> "$logfile" 2>&1
  else
    uv run jupytext --to notebook --execute --run-path . --set-kernel python3 --output "$out" "$f" >> "$logfile" 2>&1
  fi
}

run_notebook() {
  local f="$1" idx="$2"
  local logfile="$TMP/$idx.log"
  local nb_start nb_end

  nb_start=$(date +%s)
  if _jupytext_exec "$f" "$logfile"; then
    nb_end=$(date +%s)
    echo "success:$(( nb_end - nb_start ))" > "$TMP/$idx.result"
    return
  fi

  # Retry if jupytext never started reading the file — the ZMQ kernel-connection failure
  # signature. A genuine notebook failure will have "[jupytext] Reading" in the log.
  if ! grep -q '\[jupytext\] Reading' "$logfile"; then
    echo "[retry: kernel connection failure, retrying once]" >> "$logfile"
    if _jupytext_exec "$f" "$logfile"; then
      nb_end=$(date +%s)
      echo "success:$(( nb_end - nb_start ))" > "$TMP/$idx.result"
      return
    fi
  fi

  nb_end=$(date +%s)
  echo "fail:$(( nb_end - nb_start ))" > "$TMP/$idx.result"
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
