#!/usr/bin/env bash
# init-sharding.sh — Initialize MongoDB sharded cluster
#
# Run this after all services in mongo-shard-cluster.yml are healthy:
#   docker compose -f bench/docker/mongo-shard-cluster.yml up -d
#   bash bench/docker/init-sharding.sh
#
# Requires: mongosh (or run from inside a mongo:7.0 container on mongo-shard-net)

set -euo pipefail

MONGOSH="${MONGOSH:-mongosh}"

wait_primary() {
    local host="$1"
    local port="$2"
    local max_attempts=30
    local attempt=0

    echo "  Waiting for $host:$port to become primary..."
    while [ $attempt -lt $max_attempts ]; do
        result=$($MONGOSH --host "$host" --port "$port" --quiet --eval \
            'try { rs.status().myState } catch(e) { 0 }' 2>/dev/null || echo "0")
        if [ "$result" = "1" ]; then
            echo "  $host:$port is PRIMARY"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    echo "  ERROR: $host:$port did not become primary after $((max_attempts * 2))s"
    return 1
}

echo "══════════════════════════════════════════════════════════════"
echo "  MongoDB Sharded Cluster — Initialization"
echo "══════════════════════════════════════════════════════════════"

# ── Step 1: Initiate shard1 replica set ──
echo ""
echo "── Step 1: Initiate shard1 replica set (shard1rs) ──"
$MONGOSH --host localhost --port 27018 --quiet --eval '
rs.initiate({
    _id: "shard1rs",
    members: [{ _id: 0, host: "shard1:27018" }]
});
'
wait_primary localhost 27018

# ── Step 2: Initiate shard2 replica set ──
echo ""
echo "── Step 2: Initiate shard2 replica set (shard2rs) ──"
$MONGOSH --host localhost --port 27028 --quiet --eval '
rs.initiate({
    _id: "shard2rs",
    members: [{ _id: 0, host: "shard2:27028" }]
});
'
wait_primary localhost 27028

# ── Step 3: Initiate config server replica set ──
echo ""
echo "── Step 3: Initiate config server replica set (configrs) ──"
$MONGOSH --host localhost --port 27019 --quiet --eval '
rs.initiate({
    _id: "configrs",
    configsvr: true,
    members: [{ _id: 0, host: "configsvr1:27019" }]
});
'
wait_primary localhost 27019

# ── Step 4: Wait for mongos to be ready ──
echo ""
echo "── Step 4: Verify mongos router is responsive ──"
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if $MONGOSH --host localhost --port 27020 --quiet --eval 'db.adminCommand("ping")' >/dev/null 2>&1; then
        echo "  mongos router is ready on :27020"
        break
    fi
    attempt=$((attempt + 1))
    sleep 2
done
if [ $attempt -eq $max_attempts ]; then
    echo "  ERROR: mongos router not responding after $((max_attempts * 2))s"
    exit 1
fi

# ── Step 5: Add shards to the cluster ──
echo ""
echo "── Step 5: Add shards to cluster via mongos ──"
$MONGOSH --host localhost --port 27020 --quiet --eval '
sh.addShard("shard1rs/shard1:27018");
sh.addShard("shard2rs/shard2:27028");
'

# ── Step 6: Enable sharding and shard the bench collection ──
echo ""
echo "── Step 6: Enable sharding on shard_bench database ──"
$MONGOSH --host localhost --port 27020 --quiet --eval '
sh.enableSharding("shard_bench");
sh.shardCollection("shard_bench.bench", { _id: "hashed" });
'

# ── Done ──
echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  Sharded cluster initialized successfully"
echo ""
echo "  Config server:  configsvr1:27019  (configrs)"
echo "  Shard 1:        shard1:27018      (shard1rs)"
echo "  Shard 2:        shard2:27028      (shard2rs)"
echo "  Router:         mongos:27020"
echo "  Database:       shard_bench"
echo "  Collection:     shard_bench.bench  (hashed on _id)"
echo "══════════════════════════════════════════════════════════════"
