#!/usr/bin/env bash
# TurboDB — Calvin Replication Cluster Test
#
# Builds containers, starts a 3-node cluster, runs tests, tears down.
# Requirements: Docker (via Colima or native)
#
# Usage:
#   bash bench/test_calvin_cluster.sh          # full test
#   bash bench/test_calvin_cluster.sh --keep   # keep cluster running after test

set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose -f bench/docker/calvin-cluster.yml"
KEEP=false
[[ "${1:-}" == "--keep" ]] && KEEP=true

# Colors
G='\033[0;32m'; R='\033[0;31m'; Y='\033[1;33m'; B='\033[1;34m'; NC='\033[0m'

echo -e "${B}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${B}║     TurboDB Calvin Replication — Cluster Test Suite         ║${NC}"
echo -e "${B}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# ── Step 1: Build ─────────────────────────────────────────────────────────
echo -e "${Y}[1/5]${NC} Building TurboDB container image..."
$COMPOSE build --quiet 2>&1 | tail -3
echo -e "${G}      Image built.${NC}"

# ── Step 2: Start cluster ────────────────────────────────────────────────
echo -e "${Y}[2/5]${NC} Starting 3-node Calvin cluster..."
$COMPOSE up -d node-0 node-1 node-2 2>&1 | tail -3

echo -n "      Waiting for nodes to be healthy"
for i in $(seq 1 30); do
    if $COMPOSE ps --format json 2>/dev/null | python3 -c "
import sys, json
data = sys.stdin.read().strip()
# handle newline-delimited JSON
entries = [json.loads(line) for line in data.splitlines() if line.strip()]
healthy = sum(1 for e in entries if 'healthy' in e.get('Health','').lower() or 'healthy' in e.get('Status','').lower())
sys.exit(0 if healthy >= 3 else 1)
" 2>/dev/null; then
        echo ""
        echo -e "${G}      All 3 nodes healthy.${NC}"
        break
    fi
    echo -n "."
    sleep 2
done

echo ""
echo -e "      ${B}node-0${NC} (leader)  → localhost:27017"
echo -e "      ${B}node-1${NC} (replica) → localhost:27018"
echo -e "      ${B}node-2${NC} (replica) → localhost:27019"
echo ""

# ── Step 3: Run in-process Calvin E2E test ───────────────────────────────
echo -e "${Y}[3/5]${NC} Running in-process Calvin E2E test..."
echo ""
$COMPOSE run --rm tester 2>&1 | while IFS= read -r line; do
    if [[ "$line" == *"PASS"* ]]; then
        echo -e "    ${G}${line}${NC}"
    elif [[ "$line" == *"FAIL"* ]]; then
        echo -e "    ${R}${line}${NC}"
    elif [[ "$line" == *"CONSISTENT"* ]]; then
        echo -e "    ${G}${line}${NC}"
    elif [[ "$line" == *"==="* ]]; then
        echo -e "    ${B}${line}${NC}"
    else
        echo "    $line"
    fi
done

# ── Step 4: Wire protocol connectivity test ──────────────────────────────
echo ""
echo -e "${Y}[4/5]${NC} Wire protocol connectivity check..."

PASS=0
FAIL=0

for port in 27017 27018 27019; do
    name="node-$((port - 27017))"
    if nc -z localhost $port 2>/dev/null; then
        echo -e "      ${G}✓${NC} $name (port $port) — accepting connections"
        PASS=$((PASS + 1))
    else
        echo -e "      ${R}✗${NC} $name (port $port) — not reachable"
        FAIL=$((FAIL + 1))
    fi
done

echo ""

# ── Step 5: Summary ──────────────────────────────────────────────────────
echo -e "${Y}[5/5]${NC} Summary"
echo ""
echo -e "    ${B}Cluster:${NC}      3 nodes (1 leader + 2 replicas)"
echo -e "    ${B}Calvin E2E:${NC}   In-process replication test"
echo -e "    ${B}Connectivity:${NC} $PASS/$((PASS + FAIL)) nodes reachable"
echo ""

if [[ $FAIL -eq 0 ]]; then
    echo -e "${G}╔══════════════════════════════════════╗${NC}"
    echo -e "${G}║   ALL TESTS PASSED                   ║${NC}"
    echo -e "${G}╚══════════════════════════════════════╝${NC}"
else
    echo -e "${R}╔══════════════════════════════════════╗${NC}"
    echo -e "${R}║   $FAIL TEST(S) FAILED                 ║${NC}"
    echo -e "${R}╚══════════════════════════════════════╝${NC}"
fi

echo ""

# ── Cleanup ──────────────────────────────────────────────────────────────
if $KEEP; then
    echo -e "${Y}Cluster still running (--keep). Stop with:${NC}"
    echo "  $COMPOSE down -v"
else
    echo "Tearing down cluster..."
    $COMPOSE down -v 2>&1 | tail -3
    echo -e "${G}Done.${NC}"
fi
