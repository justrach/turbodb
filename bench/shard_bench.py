#!/usr/bin/env python3
"""TurboDB vs PostgreSQL vs MongoDB -- Shard Scaling Benchmark.

Compares PostgreSQL hash-partitioned tables against a MongoDB sharded cluster,
with optional TurboDB results loaded from a JSON file.

Usage:
    python bench/shard_bench.py
    python bench/shard_bench.py --docs 50000 --mongo-port 27020
"""

import os, sys, time, random, json, argparse, statistics

# -- ANSI --
G = "\033[32m"
Y = "\033[33m"
C = "\033[36m"
B = "\033[1m"
D = "\033[2m"
R = "\033[31m"
Z = "\033[0m"
SEP = "\u2500" * 80

# -- Constants --
DOCS = 10_000
DOC_SZ = 256


def rand_doc(size):
    return os.urandom(size).hex()[:size]


def pct(lats, percentile):
    if not lats:
        return 0
    s = sorted(lats)
    k = (len(s) - 1) * (percentile / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(s) else f
    return s[f] + (k - f) * (s[c] - s[f])


def fmt(n):
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return f"{n:.0f}"


def fmt_us(us):
    if us >= 1_000_000:
        return f"{us/1_000_000:.1f}s"
    if us >= 1_000:
        return f"{us/1_000:.1f}ms"
    return f"{us:.0f}\u00b5s"


# -- Engines --


class PostgresPartitioned:
    name = "Postgres"
    color = C  # cyan

    def __init__(self):
        import psycopg2

        self.conn = psycopg2.connect("dbname=shard_bench")
        self.conn.autocommit = True

    def setup_partitions(self, n):
        """Drop and recreate table with N hash partitions."""
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS bench CASCADE")
        cur.execute(
            "CREATE TABLE bench (key TEXT NOT NULL, data JSONB) PARTITION BY HASH (key)"
        )
        for i in range(n):
            cur.execute(
                f"CREATE TABLE bench_p{i} PARTITION OF bench "
                f"FOR VALUES WITH (MODULUS {n}, REMAINDER {i})"
            )
        cur.execute("CREATE INDEX ON bench (key)")
        cur.close()

    def insert(self, key, val):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO bench (key, data) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (key, json.dumps(val)),
        )
        cur.close()

    def get(self, key):
        cur = self.conn.cursor()
        cur.execute("SELECT data FROM bench WHERE key = %s", (key,))
        r = cur.fetchone()
        cur.close()
        return r

    def scan(self, limit=100):
        cur = self.conn.cursor()
        cur.execute("SELECT key, data FROM bench ORDER BY key LIMIT %s", (limit,))
        r = cur.fetchall()
        cur.close()
        return r

    def drop(self):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM bench")
        cur.close()

    def close(self):
        self.conn.close()


class MongoSharded:
    name = "MongoDB"
    color = Y  # yellow

    def __init__(self, port=27020):
        import pymongo

        self.cli = pymongo.MongoClient(
            f"mongodb://localhost:{port}/", serverSelectionTimeoutMS=5000
        )
        self.db = self.cli["shard_bench"]
        self.col = self.db["bench"]

    def setup_partitions(self, n):
        """Pre-split chunks to approximate N partitions."""
        self.col.drop()
        try:
            self.cli.admin.command(
                "shardCollection",
                "shard_bench.bench",
                key={"_id": "hashed"},
                numInitialChunks=n,
            )
        except Exception:
            # Already sharded, just drop and recreate
            pass

    def insert(self, key, val):
        self.col.insert_one({"_id": key, "data": val})

    def get(self, key):
        return self.col.find_one({"_id": key})

    def scan(self, limit=100):
        return list(self.col.find().sort("_id", 1).limit(limit))

    def drop(self):
        self.col.delete_many({})

    def close(self):
        self.cli.close()


# -- Workload runner --


def run_workload(db, workload, docs, doc_sz):
    val = rand_doc(doc_sz)
    keys = [f"d{i:09d}" for i in range(docs)]
    lats = []

    if workload == "insert":
        db.drop()
        for k in keys:
            t0 = time.perf_counter()
            db.insert(k, val)
            lats.append((time.perf_counter() - t0) * 1e6)
    elif workload == "get":
        for k in random.choices(keys, k=docs):
            t0 = time.perf_counter()
            db.get(k)
            lats.append((time.perf_counter() - t0) * 1e6)
    elif workload == "scan":
        for _ in range(docs // 10):
            t0 = time.perf_counter()
            db.scan(100)
            lats.append((time.perf_counter() - t0) * 1e6)

    total_s = sum(lats) / 1e6 if lats else 1
    return {
        "ops": len(lats),
        "ops_sec": len(lats) / total_s,
        "median": pct(lats, 50),
        "p95": pct(lats, 95),
        "p99": pct(lats, 99),
        "total_s": total_s,
    }


# -- Main --


def main():
    ap = argparse.ArgumentParser(
        description="TurboDB vs PostgreSQL vs MongoDB -- Shard Scaling Benchmark"
    )
    ap.add_argument("--docs", type=int, default=DOCS)
    ap.add_argument("--mongo-port", type=int, default=27020)
    ap.add_argument("--turbodb-json", default="/tmp/turbodb_shard_bench.json")
    args = ap.parse_args()

    partition_counts = [1, 2, 4, 8, 16]
    workloads = ["insert", "get", "scan"]

    # -- Header --
    print(f"\n{SEP}")
    print(f"{B}  TurboDB vs PostgreSQL vs MongoDB \u2014 Shard Scaling Benchmark{Z}")
    print(f"{SEP}")
    print(f"  docs: {fmt(args.docs)}  payload: {DOC_SZ}B  partitions: {partition_counts}\n")

    # -- Connect engines --
    engines = []

    # PostgreSQL
    try:
        pdb = PostgresPartitioned()
        engines.append(("Postgres", pdb, C))
        print(f"  {C}PostgreSQL ready{Z}")
    except Exception as e:
        print(f"  {R}PostgreSQL unavailable: {e}{Z}")
        pdb = None

    # MongoDB
    try:
        mdb = MongoSharded(args.mongo_port)
        mdb.cli.admin.command("ping")
        engines.append(("MongoDB", mdb, Y))
        print(f"  {Y}MongoDB (sharded) ready on :{args.mongo_port}{Z}")
    except Exception as e:
        print(f"  {R}MongoDB unavailable: {e}{Z}")
        mdb = None

    # TurboDB from JSON
    turbodb_data = None
    try:
        with open(args.turbodb_json) as f:
            turbodb_data = json.load(f)
        print(f"  {G}TurboDB results loaded from {args.turbodb_json}{Z}")
    except Exception as e:
        print(f"  {R}TurboDB JSON unavailable: {e}{Z}")

    all_results = {}  # {partition_count: {workload: {engine_name: stats}}}

    try:
        for n_parts in partition_counts:
            print(
                f"\n{B}\u2550\u2550 {n_parts} PARTITION{'S' if n_parts > 1 else ''} "
                f"{'\u2550' * (68 - len(str(n_parts)))}\u2550\u2550\u2550{Z}"
            )

            # Setup partitions for each engine
            for name, db, _ in engines:
                print(f"  {D}Setting up {name} with {n_parts} partitions...{Z}")
                db.setup_partitions(n_parts)

            part_results = {}
            for wl in workloads:
                print(f"\n  {B}\u2500\u2500 {wl.upper()} \u2500\u2500{Z}")

                wl_results = {}

                for name, db, color in engines:
                    print(f"    {D}{name}...{Z}", end=" ", flush=True)
                    s = run_workload(db, wl, args.docs, DOC_SZ)
                    wl_results[name] = s
                    print(f"{color}{fmt(s['ops_sec'])}/s{Z}  ({s['total_s']:.1f}s)")

                # Add TurboDB from JSON
                if turbodb_data:
                    wl_map = {"insert": "insert", "get": "get", "scan": "scan"}
                    json_key = f"{wl_map.get(wl, wl)}_{n_parts}p_ops_sec"
                    benchmarks = turbodb_data.get("benchmarks", {})
                    if json_key in benchmarks:
                        ops_sec = benchmarks[json_key]
                        wl_results["TurboDB"] = {
                            "ops": 0,
                            "ops_sec": ops_sec,
                            "median": 0,
                            "p95": 0,
                            "p99": 0,
                            "total_s": 0,
                        }
                        print(f"    {G}TurboDB...{Z} {G}{fmt(ops_sec)}/s{Z}  (from JSON)")

                part_results[wl] = wl_results
            all_results[n_parts] = part_results

        # -- Summary scaling table --
        print(f"\n{SEP}")
        print(f"{B}  Scaling Summary \u2014 {fmt(args.docs)} docs, {DOC_SZ}B payload{Z}")
        print(f"{SEP}")

        for wl in workloads:
            print(f"\n  {B}{wl.upper()}{Z}")
            print(f"  {'Partitions':<12}", end="")
            engine_names = set()
            for n_parts in partition_counts:
                for name in all_results.get(n_parts, {}).get(wl, {}):
                    engine_names.add(name)
            engine_names = sorted(engine_names)
            for name in engine_names:
                print(f" {name:>14}", end="")
            print()
            print(f"  {'\u2500' * 12}", end="")
            for _ in engine_names:
                print(f" {'\u2500' * 14}", end="")
            print()

            for n_parts in partition_counts:
                print(f"  {n_parts:<12}", end="")
                for name in engine_names:
                    ops = (
                        all_results.get(n_parts, {})
                        .get(wl, {})
                        .get(name, {})
                        .get("ops_sec", 0)
                    )
                    print(f" {fmt(ops):>12}/s", end="")
                print()

        # -- Head-to-head ratios --
        print(f"\n{SEP}")
        print(f"{B}  TurboDB Speedup{Z}")
        print(f"{SEP}")

        if turbodb_data:
            for wl in workloads:
                print(f"\n  {B}{wl.upper()}{Z}")
                for n_parts in partition_counts:
                    wl_res = all_results.get(n_parts, {}).get(wl, {})
                    turbo_ops = wl_res.get("TurboDB", {}).get("ops_sec", 0)
                    if turbo_ops:
                        line = f"  {n_parts:>2}p:"
                        for name in ["Postgres", "MongoDB"]:
                            other_ops = wl_res.get(name, {}).get("ops_sec", 0)
                            if other_ops:
                                ratio = turbo_ops / other_ops
                                mark = f"{G}+" if ratio >= 1.0 else f"{R}-"
                                line += f"  {mark}{Z} vs {name}: {ratio:.1f}x"
                        print(line)

        # -- Write JSON --
        json_out = {"partition_counts": partition_counts, "results": {}}
        for n_parts in partition_counts:
            for wl in workloads:
                for name, stats in all_results.get(n_parts, {}).get(wl, {}).items():
                    key = f"{name.lower()}_{wl}_{n_parts}p"
                    json_out["results"][key] = stats.get("ops_sec", 0)

        json_path = "/tmp/turbodb_shard_comparison.json"
        with open(json_path, "w") as f:
            json.dump(json_out, f, indent=2)
        print(f"\n  JSON written to {json_path}\n")

    finally:
        for name, db, _ in engines:
            db.close()


if __name__ == "__main__":
    main()
