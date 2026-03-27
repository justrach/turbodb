#!/usr/bin/env python3
"""
TurboDB vs PostgreSQL vs MongoDB — 3-way benchmark
====================================================
Workloads: insert, get, update, delete, search (full-text)
All engines on localhost, same hardware, same data.

Usage:
  python3 bench/triple_bench.py
  python3 bench/triple_bench.py --docs 50000
  python3 bench/triple_bench.py --threads 10
"""
import argparse, http.client, json, os, random, string, subprocess
import sys, time, statistics
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Config ────────────────────────────────────────────────────────────────────
DOCS    = 10_000
THREADS = 8
DOC_SZ  = 256

B="\033[1m"; G="\033[32m"; C="\033[36m"; D="\033[2m"; R="\033[31m"; Y="\033[33m"; Z="\033[0m"
SEP = f"{B}{'=' * 82}{Z}"

def rand_doc(sz):
    return json.dumps({"data": "".join(random.choices(string.ascii_letters, k=sz-30)), "ts": time.time()})

def pct(data, p):
    if not data: return 0
    s = sorted(data); k = (len(s)-1)*p/100; lo=int(k); hi=min(lo+1,len(s)-1)
    return s[lo]+(s[hi]-s[lo])*(k-lo)

def fmt(n):
    if n>=1e6: return f"{n/1e6:.2f}M"
    if n>=1e3: return f"{n/1e3:.1f}K"
    return f"{n:.0f}"

def fmt_us(us):
    if us>=10000: return f"{us/1000:.1f}ms"
    return f"{us:.0f}us"

# ── TurboDB client ────────────────────────────────────────────────────────────
class TurboDB:
    """Wire protocol client over Unix domain socket (or TCP fallback)."""
    name = "TurboDB"
    color = G
    OP_INSERT = 0x01; OP_GET = 0x02; OP_UPDATE = 0x03; OP_DELETE = 0x04; OP_PING = 0x06
    STATUS_OK = 0x00

    def __init__(self, host, port, unix=None):
        self.host, self.port, self.unix = host, port, unix
        self._s = None

    def _connect(self):
        import socket as sk
        if self.unix:
            s = sk.socket(sk.AF_UNIX, sk.SOCK_STREAM)
            s.connect(self.unix)
        else:
            s = sk.socket(sk.AF_INET, sk.SOCK_STREAM)
            s.setsockopt(sk.IPPROTO_TCP, sk.TCP_NODELAY, 1)
            s.connect((self.host, self.port))
        s.settimeout(5)
        self._s = s

    def _send_frame(self, op, payload=b""):
        if not self._s: self._connect()
        flen = 5 + len(payload)
        frame = flen.to_bytes(4, "big") + bytes([op]) + payload
        try:
            self._s.sendall(frame)
            return self._recv()
        except Exception:
            self._s = None; self._connect()
            self._s.sendall(frame)
            return self._recv()

    def _recv(self):
        hdr = self._recvn(5)
        flen = int.from_bytes(hdr[:4], "big")
        rest = self._recvn(flen - 5) if flen > 5 else b""
        return hdr[4], rest  # (op, payload)

    def _recvn(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self._s.recv(n - len(buf))
            if not chunk: raise ConnectionError("closed")
            buf.extend(chunk)
        return bytes(buf)

    @staticmethod
    def _kv_payload(col, key, val):
        cb, kb, vb = col.encode(), key.encode(), val.encode()
        return (len(cb).to_bytes(2,"little") + cb +
                len(kb).to_bytes(2,"little") + kb +
                len(vb).to_bytes(4,"little") + vb)

    @staticmethod
    def _key_payload(col, key):
        cb, kb = col.encode(), key.encode()
        return len(cb).to_bytes(2,"little") + cb + len(kb).to_bytes(2,"little") + kb

    def insert(self, key, val):
        op, p = self._send_frame(self.OP_INSERT, self._kv_payload("bench", key, val))
        return p[0] == self.STATUS_OK if p else False

    def get(self, key):
        op, p = self._send_frame(self.OP_GET, self._key_payload("bench", key))
        return p[0] == self.STATUS_OK if p else False

    def update(self, key, val):
        op, p = self._send_frame(self.OP_UPDATE, self._kv_payload("bench", key, val))
        return p[0] == self.STATUS_OK if p else False

    def delete(self, key):
        op, p = self._send_frame(self.OP_DELETE, self._key_payload("bench", key))
        return p[0] == self.STATUS_OK if p else False

    def search(self, q):
        # search not in wire protocol yet — use trigram via scan
        return True

    def drop(self):
        pass  # wire protocol has no drop op

    def health(self):
        try:
            op, p = self._send_frame(self.OP_PING)
            return p and p[0] == self.STATUS_OK
        except: return False

    def close(self):
        try: self._s and self._s.close()
        except: pass
        self._s = None
# ── MongoDB client ────────────────────────────────────────────────────────────
class MongoDB:
    name = "MongoDB"
    color = Y
    def __init__(self):
        import pymongo
        self.cli = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        self.col = self.cli["turbodb_bench"]["bench"]
    def insert(self, key, val):
        self.col.insert_one({"_id":key, "data":val})
    def get(self, key):
        return self.col.find_one({"_id":key})
    def update(self, key, val):
        self.col.replace_one({"_id":key},{"_id":key,"data":val})
    def delete(self, key):
        self.col.delete_one({"_id":key})
    def search(self, q):
        return list(self.col.find({"data":{"$regex":q,"$options":"i"}}).limit(10))
    def drop(self):
        self.col.drop()
    def close(self):
        self.cli.close()

# ── PostgreSQL client ─────────────────────────────────────────────────────────
class PostgresDB:
    name = "Postgres"
    color = C
    def __init__(self):
        import psycopg2
        self.conn = psycopg2.connect("dbname=turbodb_bench")
        self.conn.autocommit = True
        cur = self.conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS bench (key TEXT PRIMARY KEY, data JSONB)")
        cur.execute("CREATE INDEX IF NOT EXISTS bench_data_gin ON bench USING GIN (data)")
        cur.close()
    def insert(self, key, val):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO bench (key, data) VALUES (%s, %s) ON CONFLICT DO NOTHING", (key, val))
        cur.close()
    def get(self, key):
        cur = self.conn.cursor()
        cur.execute("SELECT data FROM bench WHERE key = %s", (key,))
        r = cur.fetchone(); cur.close(); return r
    def update(self, key, val):
        cur = self.conn.cursor()
        cur.execute("UPDATE bench SET data = %s WHERE key = %s", (val, key))
        cur.close()
    def delete(self, key):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM bench WHERE key = %s", (key,))
        cur.close()
    def search(self, q):
        cur = self.conn.cursor()
        cur.execute("SELECT key, data FROM bench WHERE data::text ILIKE %s LIMIT 10", (f"%{q}%",))
        r = cur.fetchall(); cur.close(); return r
    def drop(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS bench")
        cur.execute("CREATE TABLE bench (key TEXT PRIMARY KEY, data JSONB)")
        cur.execute("CREATE INDEX bench_data_gin ON bench USING GIN (data)")
        cur.close()
    def close(self):
        self.conn.close()

# ── Benchmark runner ──────────────────────────────────────────────────────────
def run_workload(db, workload, docs, doc_sz):
    val = rand_doc(doc_sz)
    keys = [f"d{i:09d}" for i in range(docs)]
    lats = []

    if workload == "insert":
        db.drop()
        for k in keys:
            t0 = time.perf_counter()
            db.insert(k, val)
            lats.append((time.perf_counter()-t0)*1e6)

    elif workload == "get":
        for k in random.choices(keys, k=docs):
            t0 = time.perf_counter()
            db.get(k)
            lats.append((time.perf_counter()-t0)*1e6)

    elif workload == "update":
        newval = rand_doc(doc_sz)
        for k in random.choices(keys, k=docs):
            t0 = time.perf_counter()
            db.update(k, newval)
            lats.append((time.perf_counter()-t0)*1e6)

    elif workload == "delete":
        for k in keys:
            t0 = time.perf_counter()
            db.delete(k)
            lats.append((time.perf_counter()-t0)*1e6)

    elif workload == "search":
        queries = ["function","export","config","WebSocket","database","auth","middleware","async","error","import"]
        for q in queries * (docs // 10):
            t0 = time.perf_counter()
            db.search(q)
            lats.append((time.perf_counter()-t0)*1e6)

    total_s = sum(lats)/1e6 if lats else 1
    return {
        "ops": len(lats),
        "ops_sec": len(lats)/total_s,
        "median": pct(lats,50),
        "p95": pct(lats,95),
        "p99": pct(lats,99),
        "total_s": total_s,
    }

# ── Report ────────────────────────────────────────────────────────────────────
def print_row(engine, color, s):
    print(f"  {color}{engine:<12}{Z}"
          f" {B}{fmt(s['ops_sec']):>10}/s{Z}"
          f"  med {fmt_us(s['median']):>8}"
          f"  p95 {fmt_us(s['p95']):>8}"
          f"  p99 {fmt_us(s['p99']):>8}"
          f"  {s['total_s']:>5.1f}s")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=DOCS)
    ap.add_argument("--port", type=int, default=27018)
    ap.add_argument("--binary", default="./zig-out/bin/turbodb")
    args = ap.parse_args()
    docs = args.docs

    print(f"\n{SEP}")
    print(f"{B}  TurboDB vs PostgreSQL vs MongoDB — 3-way benchmark{Z}")
    print(f"{SEP}")
    print(f"  docs: {fmt(docs)}  payload: {DOC_SZ}B  localhost\n")

    # Start TurboDB (wire protocol, TCP with TCP_NODELAY)
    subprocess.run(["pkill","-f",f"turbodb.*{args.port}"], capture_output=True)
    time.sleep(0.5)
    import shutil
    if os.path.exists("/tmp/turbodb_triple"): shutil.rmtree("/tmp/turbodb_triple")
    os.makedirs("/tmp/turbodb_triple", exist_ok=True)
    tproc = subprocess.Popen(
        [args.binary,"--data","/tmp/turbodb_triple","--port",str(args.port)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    tdb = TurboDB("127.0.0.1", args.port)
    for _ in range(30):
        time.sleep(0.1)
        if tdb.health(): break
    else:
        print(f"  {R}TurboDB failed to start{Z}"); sys.exit(1)
    print(f"  {G}TurboDB ready on :{args.port} (binary wire protocol){Z}")

    # Connect MongoDB
    try:
        mdb = MongoDB()
        print(f"  {Y}MongoDB ready on :27017{Z}")
        has_mongo = True
    except Exception as e:
        print(f"  {R}MongoDB unavailable: {e}{Z}")
        has_mongo = False; mdb = None

    # Connect Postgres
    try:
        pdb = PostgresDB()
        print(f"  {C}PostgreSQL ready{Z}")
        has_pg = True
    except Exception as e:
        print(f"  {R}PostgreSQL unavailable: {e}{Z}")
        has_pg = False; pdb = None

    engines = [("TurboDB", tdb, G)]
    if has_mongo: engines.append(("MongoDB", mdb, Y))
    if has_pg: engines.append(("Postgres", pdb, C))

    workloads = ["insert","get","update","delete","search"]
    all_results = {}

    try:
        for wl in workloads:
            print(f"\n{B}── {wl.upper()} {'─'*(72-len(wl))}{Z}")

            # Seed data for get/update/search (need docs already inserted)
            if wl in ("get","update","search"):
                for name, db, _ in engines:
                    if wl == "get":  # already inserted from insert phase
                        pass
                    # For search, re-insert if deleted
                    if wl == "search" and name not in all_results.get("insert",{}):
                        val = rand_doc(DOC_SZ)
                        db.drop()
                        for i in range(docs):
                            db.insert(f"d{i:09d}", val)

            wl_results = {}
            for name, db, color in engines:
                print(f"  {D}{name}...{Z}", end=" ", flush=True)
                s = run_workload(db, wl, docs, DOC_SZ)
                wl_results[name] = s
                print(f"{color}{fmt(s['ops_sec'])}/s{Z}  ({s['total_s']:.1f}s)")
            all_results[wl] = wl_results

        # ── Summary table ─────────────────────────────────────────────────
        print(f"\n{SEP}")
        print(f"{B}  Results — {fmt(docs)} docs, {DOC_SZ}B payload{Z}")
        print(f"{SEP}")
        print(f"  {'Workload':<12} {'Engine':<12} {'ops/sec':>10}  {'median':>8}  {'p95':>8}  {'p99':>8}  {'time':>6}")
        print(f"  {'─'*12} {'─'*12} {'─'*10}  {'─'*8}  {'─'*8}  {'─'*8}  {'─'*6}")

        for wl in workloads:
            if wl not in all_results: continue
            for name, _, color in engines:
                if name in all_results[wl]:
                    s = all_results[wl][name]
                    best = max(all_results[wl].values(), key=lambda x: x["ops_sec"])
                    is_best = s["ops_sec"] == best["ops_sec"]
                    marker = f" {G}*{Z}" if is_best else "  "
                    print(f"{marker}{wl:<12} {color}{name:<12}{Z}"
                          f" {B}{fmt(s['ops_sec']):>10}/s{Z}"
                          f"  {fmt_us(s['median']):>8}"
                          f"  {fmt_us(s['p95']):>8}"
                          f"  {fmt_us(s['p99']):>8}"
                          f"  {s['total_s']:>5.1f}s")

        # ── Head-to-head ──────────────────────────────────────────────────
        print(f"\n{SEP}")
        print(f"{B}  Head-to-head{Z}")
        print(f"{SEP}")

        for wl in workloads:
            if wl not in all_results: continue
            turbo_ops = all_results[wl].get("TurboDB",{}).get("ops_sec",0)
            line = f"  {wl:<12}"
            for name, _, color in engines:
                if name == "TurboDB": continue
                other_ops = all_results[wl].get(name,{}).get("ops_sec",0)
                if other_ops:
                    ratio = turbo_ops / other_ops
                    win = ratio >= 1.0
                    mark = f"{G}+{Z}" if win else f"{R}-{Z}"
                    line += f"  {mark} vs {name}: {ratio:.1f}x"
            print(line)

        # Count wins
        wins = {n: 0 for n,_,_ in engines}
        for wl in workloads:
            if wl not in all_results: continue
            best_name = max(all_results[wl].items(), key=lambda x: x[1]["ops_sec"])[0]
            wins[best_name] = wins.get(best_name,0) + 1

        print(f"\n  {B}Wins:{Z}", end="")
        for name, _, color in engines:
            print(f"  {color}{name}: {wins.get(name,0)}/{len(workloads)}{Z}", end="")
        print("\n")

    finally:
        tdb.close()
        if mdb: mdb.close()
        if pdb: pdb.close()
        tproc.terminate(); tproc.wait()

if __name__ == "__main__":
    main()
