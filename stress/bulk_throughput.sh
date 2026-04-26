#!/usr/bin/env bash
# Throughput via bulk endpoint: each request inserts BATCH docs in one HTTP call.
set -uo pipefail

HOST="${HOST:-localhost}"
PORT="${PORT:-27018}"
DURATION="${DURATION:-10}"
WORKERS="${WORKERS:-8}"
BATCH="${BATCH:-1000}"
COL="bulk_$$"
base="http://${HOST}:${PORT}"

# Pre-build a batch payload: BATCH lines of {"key":"...","value":...}
mk_batch() {
  local id="$1" base_idx="$2"
  local i=0
  while [ "$i" -lt "$BATCH" ]; do
    printf '{"key":"k%s-%d","value":{"v":%d}}\n' "$id" "$((base_idx+i))" "$i"
    i=$((i+1))
  done
}

worker() {
  local id="$1"
  local end=$(($(date +%s) + DURATION))
  local count=0
  local idx=0
  local batch_file
  batch_file=$(mktemp)
  while [ "$(date +%s)" -lt "$end" ]; do
    mk_batch "$id" "$idx" > "$batch_file"
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 30 \
      -X POST -H 'Content-Type: application/x-ndjson' \
      --data-binary "@${batch_file}" \
      "${base}/db/${COL}/bulk")
    if [ "$code" = "200" ]; then count=$((count+BATCH)); fi
    idx=$((idx+BATCH))
  done
  rm -f "$batch_file"
  echo "$count"
}

echo "bulk: ${WORKERS} workers × ${DURATION}s × batch=${BATCH}"
t0=$(date +%s)
out=$(mktemp)
for w in $(seq 1 "$WORKERS"); do worker "$w" >> "$out" & done
wait
t1=$(date +%s)
total=$(awk '{s+=$1} END{print s}' "$out")
elapsed=$((t1-t0))
echo "elapsed=${elapsed}s total=${total} → $((total/elapsed)) inserts/s"
rm -f "$out"
