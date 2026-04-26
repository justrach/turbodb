#!/usr/bin/env bash
# Hammer turbodb HTTP endpoint and watch for failures.
# Designed to run against the container; success = no non-2xx HTTP, no curl
# connect failures, no log "panic"/"error"/"segfault" lines.
set -uo pipefail

HOST="${HOST:-localhost}"
PORT="${PORT:-27018}"
ITERS="${ITERS:-500}"
WORKERS="${WORKERS:-16}"
COL="stress_$$"

base="http://${HOST}:${PORT}"
fail=0; ok=0; t0=$(date +%s)

worker() {
  local id="$1"
  local i=0
  while [ "$i" -lt "$ITERS" ]; do
    # Insert
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
      -X POST -H 'Content-Type: application/json' \
      -d "{\"_id\":\"k${id}-${i}\",\"v\":${i},\"who\":\"w${id}\"}" \
      "${base}/db/${COL}")
    if [ "$code" != "200" ]; then echo "FAIL insert w=${id} i=${i} http=${code}"; fail=$((fail+1)); else ok=$((ok+1)); fi

    # Scan (every 10th iter — relatively heavy)
    if [ $((i % 10)) -eq 0 ]; then
      code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "${base}/db/${COL}")
      if [ "$code" != "200" ]; then echo "FAIL scan w=${id} i=${i} http=${code}"; fail=$((fail+1)); fi
    fi

    i=$((i+1))
  done
  echo "worker ${id} done ok=${ok} fail=${fail}"
}

echo "stress: ${WORKERS} workers × ${ITERS} iters → ${base}/db/${COL}"
for w in $(seq 1 "$WORKERS"); do worker "$w" &  done
wait

t1=$(date +%s)
echo "elapsed=$((t1-t0))s"
echo "===final scan==="
curl -s --max-time 10 "${base}/db/${COL}?limit=2" | head -c 400; echo
