#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE="$SCRIPT_DIR/docker/mongo-shard-cluster.yml"
INIT_SCRIPT="$SCRIPT_DIR/docker/init-sharding.sh"

G='\033[32m'; Y='\033[33m'; C='\033[36m'; R='\033[31m'; B='\033[1m'; D='\033[2m'; Z='\033[0m'
SEP="════════════════════════════════════════════════════════════════════════════════"

echo -e "\n${SEP}"
echo -e "${B}  TurboDB Shard Benchmark — Full Setup${Z}"
echo -e "${SEP}\n"

# ── 1. Build + run TurboDB partition benchmark ────────────────────────────────
echo -e "${B}[1/5] Building and running Zig partition benchmark...${Z}"
cd "$REPO_DIR"
zig build bench-partition 2>&1 | tee /tmp/turbodb_shard_bench_build.log
echo -e "${G}  Zig partition benchmark complete.${Z}\n"

# ── 2. Start MongoDB sharded cluster ─────────────────────────────────────────
echo -e "${B}[2/5] Starting MongoDB sharded cluster...${Z}"
if ! command -v docker &>/dev/null; then
    echo -e "${R}  docker not found. Install Docker or Colima first.${Z}"
    echo -e "${D}  brew install colima docker && colima start --cpu 4 --memory 16${Z}"
    exit 1
fi

if ! docker info &>/dev/null 2>&1; then
    echo -e "${Y}  Docker daemon not running. Attempting to start Colima...${Z}"
    if command -v colima &>/dev/null; then
        colima start --cpu 4 --memory 16 2>/dev/null || true
        sleep 3
    else
        echo -e "${R}  Please start Docker or Colima manually.${Z}"
        exit 1
    fi
fi

docker compose -f "$COMPOSE" down -v 2>/dev/null || true
docker compose -f "$COMPOSE" up -d
echo -e "${D}  Waiting for containers to be healthy...${Z}"
sleep 10

# ── 3. Initialize sharding ───────────────────────────────────────────────────
echo -e "${B}[3/5] Initializing MongoDB sharding...${Z}"
bash "$INIT_SCRIPT"
echo -e "${G}  MongoDB sharded cluster ready.${Z}\n"

# ── 4. Ensure PostgreSQL is available ─────────────────────────────────────────
echo -e "${B}[4/5] Checking PostgreSQL...${Z}"
if command -v psql &>/dev/null; then
    psql -lqt 2>/dev/null | cut -d\| -f1 | grep -qw shard_bench || \
        createdb shard_bench 2>/dev/null || true
    echo -e "${C}  PostgreSQL ready (database: shard_bench)${Z}\n"
else
    echo -e "${Y}  psql not found — PostgreSQL benchmarks will be skipped.${Z}\n"
fi

# ── 5. Run Python shard comparison benchmark ──────────────────────────────────
echo -e "${B}[5/5] Running cross-engine shard benchmark...${Z}"
python3 -c "import psycopg2" 2>/dev/null || echo -e "${Y}  pip install psycopg2-binary for Postgres support${Z}"
python3 -c "import pymongo" 2>/dev/null || echo -e "${Y}  pip install pymongo for MongoDB support${Z}"

python3 "$SCRIPT_DIR/shard_bench.py" --turbodb-json /tmp/turbodb_shard_bench.json "$@"

# ── Done ──────────────────────────────────────────────────────────────────────
echo -e "\n${SEP}"
echo -e "${B}  Complete!${Z}"
echo -e "${SEP}"
echo -e "  TurboDB partition results:  /tmp/turbodb_shard_bench.json"
echo -e "  Cross-engine comparison:    /tmp/turbodb_shard_comparison.json"
echo -e ""
