#!/usr/bin/env bash
# Measure end-to-end HTTP insert throughput against the running container.
# Uses POST /db/:col with one document per request.
set -uo pipefail

HOST="${HOST:-localhost}"
PORT="${PORT:-27018}"
DURATION="${DURATION:-10}"   # seconds
WORKERS="${WORKERS:-32}"
COL="bench_$$"
base="http://${HOST}:${PORT}"

worker() {
  local id="$1"
  local end=$(($(date +%s) + DURATION))
  local count=0
  while [ "$(date +%s)" -lt "$end" ]; do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
      -X POST -H 'Content-Type: application/json' \
      -d "{\"_id\":\"k${id}-${count}\",\"v\":${count},\"who\":\"w${id}\"}" \
      "${base}/db/${COL}")
    [ "$code" = "200" ] && count=$((count+1))
  done
  echo "$count"
}

echo "throughput: ${WORKERS} workers × ${DURATION}s → ${base}/db/${COL}"
t0=$(date +%s)
out=$(mktemp)
for w in $(seq 1 "$WORKERS"); do worker "$w" >> "$out" & done
wait
t1=$(date +%s)
total=$(awk '{s+=$1} END{print s}' "$out")
elapsed=$((t1-t0))
echo "elapsed=${elapsed}s total=${total} → $((total/elapsed)) inserts/s"
rm -f "$out"
