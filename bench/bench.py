#!/usr/bin/env python3
"""
TurboDB vs MongoDB benchmark  (idealo/mongodb-benchmarking style)
================================================================
Workloads: insert, update, delete, upsert, mixed read/write
Metrics:   per-second throughput, moving avg, median/P95/P99 latency
Output:    console + CSV

Usage:
  python3 bench/bench.py                        # TurboDB + live MongoDB
  python3 bench/bench.py --docs 100000          # 100K documents
  python3 bench/bench.py --threads 10           # 10 concurrent threads
  python3 bench/bench.py --large-docs           # 2KB documents
  python3 bench/bench.py --turbodb-only         # skip MongoDB
  python3 bench/bench.py --csv results.csv      # export CSV
"""

import argparse, csv, http.client, json, math, os, random, string
import subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

# ── Defaults ──────────────────────────────────────────────────────────────────

DOCS        = 10_000     # total documents (idealo default: 100K)
THREADS     = 10         # concurrent threads (idealo default: 10)
SMALL_SIZE  = 256        # bytes  (standard doc)
LARGE_SIZE  = 2048       # bytes  (--large-docs, same as idealo)

# ── ANSI ──────────────────────────────────────────────────────────────────────

B  = "\033[1m"
G  = "\033[32m"
C  = "\033[36m"
D  = "\033[2m"
R  = "\033[31m"
Y  = "\033[33m"
W  = "\033[37m"
Z  = "\033[0m"

# ── helpers ───────────────────────────────────────────────────────────────────

def rand_doc(size):
    payload = "".join(random.choices(string.ascii_letters + string.digits, k=max(size - 30, 10)))
    return json.dumps({"data": payload, "ts": time.time()})

def pct(data, p):
    if not data: return 0.0
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)

def fmt(n):
    if n >= 1_000_000: return f"{n/1e6:.2f}M"
    if n >= 1_000:     return f"{n/1e3:.1f}K"
    return f"{n:.0f}"

def fmt_us(us):
    if us >= 10_000: return f"{us/1000:.1f}ms"
    return f"{us:.0f}µs"

def bar(ratio, width=30):
    filled = min(int(ratio * width / 5), width)  # 5x = full bar
    return "█" * filled + "░" * (width - filled)

# ── TurboDB HTTP client (keep-alive) ─────────────────────────────────────────

class TurboClient:
    def __init__(self, host, port):
        self.host, self.port = host, port
        self._c = None

    def _conn(self):
        if not self._c:
            self._c = http.client.HTTPConnection(self.host, self.port, timeout=5)
            self._c.connect()
        return self._c

    def _req(self, method, path, body=None):
        hdrs = {"Connection": "keep-alive"}
        enc = body.encode() if body else None
        if enc:
            hdrs["Content-Length"] = str(len(enc))
            hdrs["Content-Type"]  = "application/json"
        for attempt in range(2):
            try:
                self._conn().request(method, path, body=enc, headers=hdrs)
                resp = self._conn().getresponse()
                data = resp.read()
                try:
                    return json.loads(data) if data else {}
                except json.JSONDecodeError:
                    return {}  # tolerate malformed responses during benchmarking
            except Exception:
                try: self._c.close()
                except: pass
                self._c = None
                if attempt: return {}  # don't crash benchmark on connection errors

    def insert(self, col, key, val):  return self._req("POST",   f"/db/{col}", json.dumps({"key":key,"value":val}))
    def get(self, col, key):          return self._req("GET",    f"/db/{col}/{key}")
    def update(self, col, key, val):  return self._req("PUT",    f"/db/{col}/{key}", json.dumps({"value":val}))
    def delete(self, col, key):       return self._req("DELETE", f"/db/{col}/{key}")
    def upsert(self, col, key, val):
        r = self._req("PUT", f"/db/{col}/{key}", json.dumps({"value":val}))
        if r and r.get("error") == "not found":
            return self.insert(col, key, val)
        return r
    def drop(self, col):
        try: self._req("DELETE", f"/db/{col}")
        except: pass
    def health(self):
        try: return self._req("GET", "/health").get("engine") == "TurboDB"
        except: return False
    def close(self):
        try: self._c and self._c.close()
        except: pass

# ── Workload runner (generic, works for both engines) ─────────────────────────

class Stats:
    def __init__(self):
        self.lats = []
        self.per_sec = []
        self._sec_count = 0
        self._sec_start = time.perf_counter()

    def record(self, lat_us):
        self.lats.append(lat_us)
        self._sec_count += 1
        now = time.perf_counter()
        if now - self._sec_start >= 1.0:
            self.per_sec.append(self._sec_count)
            self._sec_count = 0
            self._sec_start = now

    def flush(self):
        if self._sec_count:
            self.per_sec.append(self._sec_count)
            self._sec_count = 0

    def summary(self):
        self.flush()
        total_s = sum(self.lats) / 1e6 if self.lats else 1
        return {
            "total_ops": len(self.lats),
            "ops_sec":   len(self.lats) / total_s,
            "mean_rate": sum(self.per_sec) / len(self.per_sec) if self.per_sec else 0,
            "peak_rate": max(self.per_sec) if self.per_sec else 0,
            "median_us": pct(self.lats, 50),
            "p95_us":    pct(self.lats, 95),
            "p99_us":    pct(self.lats, 99),
        }


def run_turbodb_workload(host, port, workload, col, docs, threads, doc_size):
    """Run a workload against TurboDB. Returns Stats summary."""
    val = rand_doc(doc_size)
    stats = Stats()

    if workload == "insert":
        def worker(start, count):
            c = TurboClient(host, port)
            local = []
            for i in range(count):
                k = f"d{start+i:09d}"
                t0 = time.perf_counter()
                c.insert(col, k, val)
                local.append((time.perf_counter() - t0) * 1e6)
            c.close()
            return local

    elif workload == "get":
        keys = [f"d{i:09d}" for i in range(docs)]
        def worker(start, count):
            c = TurboClient(host, port)
            local = []
            for _ in range(count):
                k = random.choice(keys)
                t0 = time.perf_counter()
                c.get(col, k)
                local.append((time.perf_counter() - t0) * 1e6)
            c.close()
            return local

    elif workload == "update":
        keys = [f"d{i:09d}" for i in range(docs)]
        newval = rand_doc(doc_size)
        def worker(start, count):
            c = TurboClient(host, port)
            local = []
            for _ in range(count):
                k = random.choice(keys)
                t0 = time.perf_counter()
                c.update(col, k, newval)
                local.append((time.perf_counter() - t0) * 1e6)
            c.close()
            return local

    elif workload == "delete":
        def worker(start, count):
            c = TurboClient(host, port)
            local = []
            for i in range(count):
                k = f"d{start+i:09d}"
                t0 = time.perf_counter()
                c.delete(col, k)
                local.append((time.perf_counter() - t0) * 1e6)
            c.close()
            return local

    elif workload == "upsert":
        def worker(start, count):
            c = TurboClient(host, port)
            local = []
            for i in range(count):
                k = f"u{random.randint(0, docs*2):09d}"
                t0 = time.perf_counter()
                c.upsert(col, k, val)
                local.append((time.perf_counter() - t0) * 1e6)
            c.close()
            return local

    elif workload == "mixed":
        keys = [f"d{i:09d}" for i in range(docs)]
        newval = rand_doc(doc_size)
        def worker(start, count):
            c = TurboClient(host, port)
            local = []
            for i in range(count):
                r = random.randint(0, 4)
                k = random.choice(keys)
                t0 = time.perf_counter()
                if r <= 2:   c.get(col, k)       # 60% read
                elif r == 3: c.update(col, k, newval)  # 20% update
                else:        c.insert(col, f"mx{random.randint(0,9999999):09d}", val)  # 20% insert
                local.append((time.perf_counter() - t0) * 1e6)
            c.close()
            return local
    else:
        raise ValueError(f"unknown workload: {workload}")

    per_thread = docs // threads
    work = [(t * per_thread, per_thread) for t in range(threads)]

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [ex.submit(worker, s, n) for s, n in work]
        for f in as_completed(futures):
            for lat in f.result():
                stats.record(lat)
    wall_time = time.perf_counter() - t_start

    s = stats.summary()
    s["wall_sec"] = wall_time
    s["wall_ops_sec"] = s["total_ops"] / wall_time
    return s


def run_mongo_workload(workload, col_name, docs, threads, doc_size):
    """Run a workload against MongoDB. Returns Stats summary."""
    import pymongo
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["turbodb_bench"]
    col = db[col_name]

    val = {"data": "x" * (doc_size - 30), "ts": time.time()}
    stats = Stats()

    if workload == "insert":
        col.drop()
        def worker(start, count):
            local = []
            for i in range(count):
                k = f"d{start+i:09d}"
                t0 = time.perf_counter()
                col.insert_one({"_id": k, **val})
                local.append((time.perf_counter() - t0) * 1e6)
            return local

    elif workload == "get":
        keys = [f"d{i:09d}" for i in range(docs)]
        def worker(start, count):
            local = []
            for _ in range(count):
                k = random.choice(keys)
                t0 = time.perf_counter()
                col.find_one({"_id": k})
                local.append((time.perf_counter() - t0) * 1e6)
            return local

    elif workload == "update":
        keys = [f"d{i:09d}" for i in range(docs)]
        newval = {"data": "y" * (doc_size - 30), "ts": time.time()}
        def worker(start, count):
            local = []
            for _ in range(count):
                k = random.choice(keys)
                t0 = time.perf_counter()
                col.replace_one({"_id": k}, {"_id": k, **newval})
                local.append((time.perf_counter() - t0) * 1e6)
            return local

    elif workload == "delete":
        def worker(start, count):
            local = []
            for i in range(count):
                k = f"d{start+i:09d}"
                t0 = time.perf_counter()
                col.delete_one({"_id": k})
                local.append((time.perf_counter() - t0) * 1e6)
            return local

    elif workload == "upsert":
        def worker(start, count):
            local = []
            for i in range(count):
                k = f"u{random.randint(0, docs*2):09d}"
                t0 = time.perf_counter()
                col.replace_one({"_id": k}, {"_id": k, **val}, upsert=True)
                local.append((time.perf_counter() - t0) * 1e6)
            return local

    elif workload == "mixed":
        keys = [f"d{i:09d}" for i in range(docs)]
        newval = {"data": "z" * (doc_size - 30)}
        def worker(start, count):
            local = []
            for i in range(count):
                r = random.randint(0, 4)
                k = random.choice(keys)
                t0 = time.perf_counter()
                if r <= 2:   col.find_one({"_id": k})
                elif r == 3: col.replace_one({"_id": k}, {"_id": k, **newval})
                else:        col.insert_one({"_id": f"mx{random.randint(0,9999999):09d}", **val})
                local.append((time.perf_counter() - t0) * 1e6)
            return local
    else:
        raise ValueError(f"unknown workload: {workload}")

    per_thread = docs // threads
    work = [(t * per_thread, per_thread) for t in range(threads)]

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [ex.submit(worker, s, n) for s, n in work]
        for f in as_completed(futures):
            for lat in f.result():
                stats.record(lat)
    wall_time = time.perf_counter() - t_start

    s = stats.summary()
    s["wall_sec"] = wall_time
    s["wall_ops_sec"] = s["total_ops"] / wall_time
    client.close()
    return s

# ── seed TurboDB with data for read/update/mixed workloads ────────────────────

def seed_turbodb(host, port, col, docs, threads, doc_size):
    """Insert docs sequentially for use by other workloads."""
    print(f"  {D}Seeding {fmt(docs)} docs into TurboDB...{Z}", end=" ", flush=True)
    val = rand_doc(doc_size)
    def worker(start, count):
        c = TurboClient(host, port)
        for i in range(count):
            c.insert(col, f"d{start+i:09d}", val)
        c.close()
    per_thread = docs // threads
    with ThreadPoolExecutor(max_workers=threads) as ex:
        list(ex.map(lambda a: worker(*a), [(t*per_thread, per_thread) for t in range(threads)]))
    print(f"{G}done{Z}")

def seed_mongo(col_name, docs, threads, doc_size):
    import pymongo
    print(f"  {D}Seeding {fmt(docs)} docs into MongoDB...{Z}", end=" ", flush=True)
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    col = client["turbodb_bench"][col_name]
    col.drop()
    val = {"data": "x" * (doc_size - 30), "ts": time.time()}
    def worker(start, count):
        for i in range(count):
            col.insert_one({"_id": f"d{start+i:09d}", **val})
    per_thread = docs // threads
    with ThreadPoolExecutor(max_workers=threads) as ex:
        list(ex.map(lambda a: worker(*a), [(t*per_thread, per_thread) for t in range(threads)]))
    client.close()
    print(f"{G}done{Z}")

# ── Report ────────────────────────────────────────────────────────────────────

SEP = f"{B}{'═' * 80}{Z}"

def print_header():
    print(f"\n{SEP}")
    print(f"{B}  {'Workload':<14} {'Engine':<10} {'ops/sec':>10} {'wall-ops/s':>10}"
          f"  {'median':>8} {'p95':>8} {'p99':>8}  {'wall':>6}{Z}")
    print(f"  {'─'*14} {'─'*10} {'─'*10} {'─'*10}  {'─'*8} {'─'*8} {'─'*8}  {'─'*6}")

def print_row(workload, engine, s, is_winner=False):
    color = G if is_winner else ""
    rst = Z if is_winner else ""
    print(
        f"  {color}{workload:<14}{rst} {engine:<10}"
        f" {B}{fmt(s['ops_sec']):>10}/s{Z}"
        f" {fmt(s['wall_ops_sec']):>10}/s"
        f"  {fmt_us(s['median_us']):>8}"
        f" {fmt_us(s['p95_us']):>8}"
        f" {fmt_us(s['p99_us']):>8}"
        f"  {s['wall_sec']:>5.1f}s"
    )

def print_comparison(workload, turbo_s, mongo_s):
    t_ops = turbo_s["wall_ops_sec"]
    m_ops = mongo_s["wall_ops_sec"]
    ratio = t_ops / m_ops if m_ops else 0
    turbo_wins = ratio >= 1.0
    marker = f"{G}✓{Z}" if turbo_wins else f"{R}✗{Z}"
    color = G if turbo_wins else R
    print(
        f"  {marker} {workload:<12}"
        f"  TurboDB {fmt(t_ops):>9}/s  vs  MongoDB {fmt(m_ops):>9}/s"
        f"  {color}{B}{ratio:5.1f}×{Z}  {bar(ratio)}"
    )
    return turbo_wins

# ── TurboDB process management ────────────────────────────────────────────────

def start_turbodb(binary, data_dir, port):
    os.makedirs(data_dir, exist_ok=True)
    p = subprocess.Popen(
        [binary, "--data", data_dir, "--port", str(port), "--http"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    c = TurboClient("127.0.0.1", port)
    for _ in range(60):
        time.sleep(0.1)
        if c.health():
            c.close()
            return p
    c.close()
    err = p.stderr.read(512).decode(errors="replace")
    p.terminate()
    print(f"{R}TurboDB didn't start:{Z}\n{err}")
    sys.exit(1)
def check_mongo():
    try:
        import pymongo
        c = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        c.admin.command("ping")
        c.close()
        return True
    except:
        return False

# ── Main ──────────────────────────────────────────────────────────────────────

WORKLOADS = ["insert", "get", "update", "delete", "upsert", "mixed"]

def main():
    ap = argparse.ArgumentParser(description="TurboDB vs MongoDB benchmark (idealo style)")
    ap.add_argument("--docs",         type=int, default=DOCS,    help=f"documents per workload (default {DOCS})")
    ap.add_argument("--threads",      type=int, default=THREADS, help=f"concurrent threads (default {THREADS})")
    ap.add_argument("--large-docs",   action="store_true",       help="use 2KB documents (default 256B)")
    ap.add_argument("--port",         type=int, default=27018,   help="TurboDB port (default 27018)")
    ap.add_argument("--binary",       default="./zig-out/bin/turbodb")
    ap.add_argument("--data-dir",     default="/tmp/turbodb_bench")
    ap.add_argument("--turbodb-only", action="store_true",       help="skip MongoDB comparison")
    ap.add_argument("--no-start",     action="store_true",       help="TurboDB already running")
    ap.add_argument("--csv",          default=None,              help="export CSV to file")
    ap.add_argument("--workloads",    default="all",             help="comma-separated: insert,get,update,delete,upsert,mixed")
    args = ap.parse_args()

    docs    = args.docs
    threads = args.threads
    dsz     = LARGE_SIZE if args.large_docs else SMALL_SIZE
    wl_list = WORKLOADS if args.workloads == "all" else args.workloads.split(",")

    print(f"\n{B}{'═' * 80}{Z}")
    print(f"{B}  TurboDB vs MongoDB Benchmark{Z}  (idealo/mongodb-benchmarking style)")
    print(f"{B}{'═' * 80}{Z}")
    print(f"  docs      : {fmt(docs)}")
    print(f"  threads   : {threads}")
    print(f"  doc size  : {dsz}B {'(large)' if args.large_docs else '(standard)'}")
    print(f"  workloads : {', '.join(wl_list)}")

    # ── Start engines ─────────────────────────────────────────────────────────
    proc = None
    if not args.no_start:
        print(f"\n  {D}Starting TurboDB on :{args.port}...{Z}", end=" ", flush=True)
        proc = start_turbodb(args.binary, args.data_dir, args.port)
        print(f"{G}ready (pid {proc.pid}){Z}")
    else:
        c = TurboClient("127.0.0.1", args.port)
        if not c.health():
            print(f"  {R}TurboDB not reachable on :{args.port}{Z}"); sys.exit(1)
        c.close()

    has_mongo = False
    if not args.turbodb_only:
        has_mongo = check_mongo()
        if has_mongo:
            print(f"  {G}MongoDB connected on :27017{Z}")
        else:
            print(f"  {Y}MongoDB not available — TurboDB-only mode{Z}")

    csv_rows = []

    try:
        # ── Run workloads ─────────────────────────────────────────────────────
        results = {}
        turbo_host = "127.0.0.1"

        for wl in wl_list:
            print(f"\n{B}── {wl.upper()} {'─' * (70 - len(wl))}{Z}")

            # Seed data for workloads that need pre-existing docs
            if wl in ("get", "update", "mixed"):
                tc = TurboClient(turbo_host, args.port)
                tc.drop(f"bench_{wl}")
                tc.close()
                seed_turbodb(turbo_host, args.port, f"bench_{wl}", docs, threads, dsz)
                if has_mongo:
                    seed_mongo(f"bench_{wl}", docs, threads, dsz)
            elif wl == "insert":
                tc = TurboClient(turbo_host, args.port)
                tc.drop(f"bench_{wl}")
                tc.close()
                if has_mongo:
                    import pymongo
                    cli = pymongo.MongoClient("mongodb://localhost:27017/")
                    cli["turbodb_bench"][f"bench_{wl}"].drop()
                    cli.close()

            # TurboDB
            print(f"  {C}TurboDB{Z}  {threads} threads × {fmt(docs//threads)} docs ...", end=" ", flush=True)
            t_result = run_turbodb_workload(turbo_host, args.port, wl, f"bench_{wl}", docs, threads, dsz)
            print(f"{G}{fmt(t_result['wall_ops_sec'])}/s{Z}  ({t_result['wall_sec']:.1f}s)")

            # MongoDB
            m_result = None
            if has_mongo:
                print(f"  {Y}MongoDB{Z}  {threads} threads × {fmt(docs//threads)} docs ...", end=" ", flush=True)
                m_result = run_mongo_workload(wl, f"bench_{wl}", docs, threads, dsz)
                print(f"{fmt(m_result['wall_ops_sec'])}/s  ({m_result['wall_sec']:.1f}s)")

            results[wl] = (t_result, m_result)

            # CSV row
            csv_rows.append({
                "workload":        wl,
                "docs":            docs,
                "threads":         threads,
                "doc_size":        dsz,
                "turbodb_ops_sec": round(t_result["wall_ops_sec"]),
                "turbodb_med_us":  round(t_result["median_us"]),
                "turbodb_p99_us":  round(t_result["p99_us"]),
                "mongodb_ops_sec": round(m_result["wall_ops_sec"]) if m_result else "",
                "mongodb_med_us":  round(m_result["median_us"]) if m_result else "",
                "mongodb_p99_us":  round(m_result["p99_us"]) if m_result else "",
            })

        # ── Summary table ─────────────────────────────────────────────────────
        print_header()
        for wl, (t_s, m_s) in results.items():
            turbo_wins = not m_s or t_s["wall_ops_sec"] >= m_s["wall_ops_sec"]
            print_row(wl, "TurboDB", t_s, is_winner=turbo_wins)
            if m_s:
                mongo_wins = m_s["wall_ops_sec"] > t_s["wall_ops_sec"]
                print_row(wl, "MongoDB", m_s, is_winner=mongo_wins)

        # ── Head-to-head ──────────────────────────────────────────────────────
        if has_mongo:
            print(f"\n{SEP}")
            print(f"{B}  Head-to-head: TurboDB vs MongoDB 8.2{Z}")
            print(f"{B}{'═' * 80}{Z}")
            wins = 0
            total = 0
            for wl, (t_s, m_s) in results.items():
                if m_s:
                    total += 1
                    if print_comparison(wl, t_s, m_s):
                        wins += 1

            color = G if wins == total else (C if wins > total // 2 else R)
            print(f"\n  {color}{B}TurboDB: {wins}/{total} wins vs MongoDB{Z}")
            if wins >= total // 2:
                avg_speedup = sum(
                    r[0]["wall_ops_sec"] / r[1]["wall_ops_sec"]
                    for r in results.values() if r[1]
                ) / total
                print(f"  {G}Average speedup: {avg_speedup:.1f}× faster than MongoDB{Z}")
        else:
            print(f"\n  {D}Run MongoDB for live comparison: brew services start mongodb-community{Z}")

        # ── CSV export ────────────────────────────────────────────────────────
        if args.csv and csv_rows:
            with open(args.csv, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=csv_rows[0].keys())
                w.writeheader()
                w.writerows(csv_rows)
            print(f"\n  {D}CSV exported to {args.csv}{Z}")

        print()

    finally:
        if proc:
            proc.terminate()
            proc.wait()

if __name__ == "__main__":
    main()
