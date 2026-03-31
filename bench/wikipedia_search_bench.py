#!/usr/bin/env python3.13
"""
Unified Wikipedia Search Benchmark: TurboDB vs PostgreSQL vs MongoDB vs MySQL
=============================================================================
Same 2K Wikipedia articles, same 5 benchmarks, head-to-head comparison.

Usage:
  python3.13 bench/wikipedia_search_bench.py [--docs N]

Requires: psycopg2, pymongo, pymysql, TurboDB binary at ./zig-out/bin/turbodb
"""

import json, os, sys, time, random, re, argparse, subprocess, shutil
import http.client
from statistics import median
from urllib.parse import quote
from abc import ABC, abstractmethod

B="\033[1m"; G="\033[32m"; C="\033[36m"; D="\033[2m"
Y="\033[33m"; R="\033[31m"; M="\033[35m"; Z="\033[0m"

CACHE = "/tmp/turbodb_wiki_data.jsonl"
QUERIES = [
    "United States", "quantum physics", "machine learning",
    "solar system", "world war", "climate change",
    "photosynthesis", "democracy", "chromosome",
    "algorithm", "electromagnetic", "biodiversity",
    "artificial intelligence", "human rights",
    "carbon dioxide", "natural selection",
    "mitochondria", "fibonacci", "superconductor",
    "paleontology", "cryptography",
]

def load_articles(n):
    if not os.path.exists(CACHE):
        print(f"{R}No cached data. Run wikipedia_bench.py first.{Z}"); sys.exit(1)
    docs = []
    with open(CACHE) as f:
        for line in f:
            if len(docs) >= n: break
            o = json.loads(line); docs.append((o["title"], o["text"]))
    return docs

def fmt_ops(v):
    if v >= 1e6: return f"{v/1e6:.1f}M"
    if v >= 1e3: return f"{v/1e3:.1f}K"
    return f"{v:.0f}"

def fmt_ms(ms):
    if ms < 0.001: return f"{ms*1e6:.0f}ns"
    if ms < 1: return f"{ms*1000:.0f}\u00b5s"
    if ms < 1000: return f"{ms:.1f}ms"
    return f"{ms/1000:.2f}s"

def pct(arr, p):
    s = sorted(arr)
    return s[min(int(len(s)*p/100), len(s)-1)]


# ═══════════════════════════════════════════════════════════════════════════════
# Engine base class
# ═══════════════════════════════════════════════════════════════════════════════

class Engine(ABC):
    name = "?"
    @abstractmethod
    def setup(self, docs): ...
    @abstractmethod
    def insert_one(self, key, title, text, length): ...
    @abstractmethod
    def get_one(self, key): ...
    @abstractmethod
    def scan_page(self, limit, offset): ...
    @abstractmethod
    def search_fts(self, query, limit=20): ...
    @abstractmethod
    def search_substr(self, query, limit=20): ...
    @abstractmethod
    def teardown(self): ...


# ═══════════════════════════════════════════════════════════════════════════════
# TurboDB (FFI — direct in-process, no HTTP)
# ═══════════════════════════════════════════════════════════════════════════════

class TurboDBEngine(Engine):
    name = "TurboDB"

    def __init__(self):
        self.data_dir = "/tmp/wiki_bench_turbodb"
        self.db = None
        self.col = None

    def setup(self, docs):
        # Import TurboDB Python SDK (FFI via ctypes -> libturbodb.dylib)
        bench_dir = os.path.dirname(os.path.abspath(__file__))
        sdk_path = os.path.join(bench_dir, "..", "python")
        if sdk_path not in sys.path:
            sys.path.insert(0, sdk_path)
        from turbodb import Database

        if os.path.exists(self.data_dir): shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.db = Database(self.data_dir)
        self.col = self.db.collection("wiki")

    def insert_one(self, key, title, text, length):
        val = json.dumps({"title": title, "text": text, "length": length})
        try:
            self.col.insert(key, val)
        except Exception:
            pass  # skip dupes

    def _create_indexes(self):
        # Wait for background indexer thread to finish building trigram+word indexes.
        # The Zig background thread processes ~1ms/doc, so 2K docs ~ 2s.
        import time
        time.sleep(3)

    def get_one(self, key):
        return self.col.get(key)

    def scan_page(self, limit, offset):
        return self.col.scan(limit=limit, offset=offset)

    def search_fts(self, query, limit=20):
        return self.col.search(query, limit=limit)

    def search_substr(self, query, limit=20):
        return self.col.search(query, limit=limit)  # trigram IS substring

    def teardown(self):
        if self.db:
            self.db.close()
            self.db = None


# ═══════════════════════════════════════════════════════════════════════════════
# PostgreSQL
# ═══════════════════════════════════════════════════════════════════════════════

class PostgresEngine(Engine):
    name = "PostgreSQL"

    def setup(self, docs):
        import psycopg2
        # Create database
        c = psycopg2.connect("dbname=postgres"); c.autocommit = True
        cur = c.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname='wiki_bench'")
        if not cur.fetchone():
            cur.execute("CREATE DATABASE wiki_bench")
        cur.close(); c.close()

        self.conn = psycopg2.connect("dbname=wiki_bench")
        self.conn.autocommit = True
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS articles")
        cur.execute("CREATE TABLE articles (key TEXT PRIMARY KEY, title TEXT, text TEXT, length INT)")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        cur.close()

    def _create_indexes(self):
        cur = self.conn.cursor()
        cur.execute("CREATE INDEX IF NOT EXISTS idx_text_fts ON articles USING GIN (to_tsvector('english', text))")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_text_trgm ON articles USING GIN (text gin_trgm_ops)")
        cur.close()

    def insert_one(self, key, title, text, length):
        cur = self.conn.cursor()
        try:
            cur.execute("INSERT INTO articles (key, title, text, length) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                        (key, title, text, length))
        finally: cur.close()

    def get_one(self, key):
        cur = self.conn.cursor()
        cur.execute("SELECT key, title, length FROM articles WHERE key=%s", (key,))
        row = cur.fetchone(); cur.close()
        return row

    def scan_page(self, limit, offset):
        cur = self.conn.cursor()
        cur.execute("SELECT key, title, length FROM articles ORDER BY key LIMIT %s OFFSET %s", (limit, offset))
        rows = cur.fetchall(); cur.close()
        return rows

    def search_fts(self, query, limit=20):
        cur = self.conn.cursor()
        cur.execute("SELECT key, title FROM articles WHERE to_tsvector('english', text) @@ plainto_tsquery('english', %s) LIMIT %s",
                    (query, limit))
        rows = cur.fetchall(); cur.close()
        return rows

    def search_substr(self, query, limit=20):
        cur = self.conn.cursor()
        cur.execute("SELECT key, title FROM articles WHERE text ILIKE %s LIMIT %s",
                    (f"%{query}%", limit))
        rows = cur.fetchall(); cur.close()
        return rows

    def teardown(self):
        if hasattr(self, 'conn') and self.conn:
            try:
                cur = self.conn.cursor()
                cur.execute("DROP TABLE IF EXISTS articles")
                cur.close()
                self.conn.close()
            except: pass


# ═══════════════════════════════════════════════════════════════════════════════
# MongoDB
# ═══════════════════════════════════════════════════════════════════════════════

class MongoEngine(Engine):
    name = "MongoDB"

    def setup(self, docs):
        import pymongo
        self.client = pymongo.MongoClient("localhost", 27017)
        self.db = self.client["wiki_bench"]
        self.col = self.db["articles"]
        self.col.drop()

    def _create_indexes(self):
        self.col.create_index([("text", "text")])

    def insert_one(self, key, title, text, length):
        import pymongo
        try:
            self.col.insert_one({"_id": key, "title": title, "text": text, "length": length})
        except pymongo.errors.DuplicateKeyError: pass

    def get_one(self, key):
        return self.col.find_one({"_id": key})

    def scan_page(self, limit, offset):
        return list(self.col.find().skip(offset).limit(limit))

    def search_fts(self, query, limit=20):
        return list(self.col.find({"$text": {"$search": query}}).limit(limit))

    def search_substr(self, query, limit=20):
        escaped = re.escape(query)
        return list(self.col.find({"text": {"$regex": escaped, "$options": "i"}}).limit(limit))

    def teardown(self):
        if hasattr(self, 'col'):
            try: self.col.drop()
            except: pass
        if hasattr(self, 'client'):
            self.client.close()


# ═══════════════════════════════════════════════════════════════════════════════
# MySQL
# ═══════════════════════════════════════════════════════════════════════════════

class MySQLEngine(Engine):
    name = "MySQL"

    def setup(self, docs):
        import pymysql
        c = pymysql.connect(host="localhost", user="root")
        cur = c.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS wiki_bench")
        cur.close(); c.close()

        self.conn = pymysql.connect(host="localhost", user="root", database="wiki_bench", autocommit=True)
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS articles")
        cur.execute("""CREATE TABLE articles (
            key_ VARCHAR(255) PRIMARY KEY,
            title VARCHAR(500),
            text_ MEDIUMTEXT,
            length_ INT,
            FULLTEXT INDEX ft_text (text_)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
        cur.close()

    def insert_one(self, key, title, text, length):
        cur = self.conn.cursor()
        try:
            cur.execute("INSERT IGNORE INTO articles (key_, title, text_, length_) VALUES (%s,%s,%s,%s)",
                        (key, title, text, length))
        finally: cur.close()

    def get_one(self, key):
        cur = self.conn.cursor()
        cur.execute("SELECT key_, title, length_ FROM articles WHERE key_=%s", (key,))
        row = cur.fetchone(); cur.close()
        return row

    def scan_page(self, limit, offset):
        cur = self.conn.cursor()
        cur.execute("SELECT key_, title, length_ FROM articles LIMIT %s OFFSET %s", (limit, offset))
        rows = cur.fetchall(); cur.close()
        return rows

    def search_fts(self, query, limit=20):
        # In BOOLEAN MODE, prefix each word with + for mandatory matching
        bool_query = " ".join(f"+{w}" for w in query.split())
        cur = self.conn.cursor()
        cur.execute("SELECT key_, title FROM articles WHERE MATCH(text_) AGAINST(%s IN BOOLEAN MODE) LIMIT %s",
                    (bool_query, limit))
        rows = cur.fetchall(); cur.close()
        return rows

    def search_substr(self, query, limit=20):
        cur = self.conn.cursor()
        cur.execute("SELECT key_, title FROM articles WHERE text_ LIKE %s LIMIT %s",
                    (f"%{query}%", limit))
        rows = cur.fetchall(); cur.close()
        return rows

    def teardown(self):
        if hasattr(self, 'conn') and self.conn:
            try: self.conn.close()
            except: pass


# ═══════════════════════════════════════════════════════════════════════════════
# Benchmark runner
# ═══════════════════════════════════════════════════════════════════════════════

def run_benchmark(engine, docs, keys):
    """Run all benchmarks against one engine. Returns dict of results."""
    sep = "=" * 60
    name = engine.name

    print(f"\n{M}{sep}{Z}")
    print(f"{M}  {B}{name}{Z}")
    print(f"{M}{sep}{Z}")

    results = {}

    # ── INSERT ──
    print(f"\n  {B}Insert ({len(docs):,} docs){Z}")
    engine.setup(docs)
    t0 = time.perf_counter(); times = []; ins = 0
    for title, text in docs:
        key = title.replace("/", "_").replace(" ", "_")[:128].encode("ascii", "ignore").decode()
        bt = time.perf_counter()
        engine.insert_one(key, title, text, len(text))
        times.append((time.perf_counter() - bt) * 1000)
        ins += 1
        if ins % 500 == 0:
            print(f"\r    {D}{ins:,}/{len(docs):,} ({ins/(time.perf_counter()-t0):,.0f}/s){Z}", end="", flush=True)
    elapsed = time.perf_counter() - t0
    rate = ins / elapsed; med = median(times); p95 = pct(times, 95)
    print(f"\r    {G}{ins:,} docs in {elapsed:.2f}s{Z} \u2014 {B}{fmt_ops(rate)}/s{Z}  med={fmt_ms(med)}  p95={fmt_ms(p95)}")
    results["insert"] = {"ops_sec": rate, "median_ms": med, "p95_ms": p95}

    # Create indexes after insert (fairer — Postgres/MySQL build index separately)
    if hasattr(engine, '_create_indexes'):
        t0 = time.perf_counter()
        engine._create_indexes()
        idx_t = time.perf_counter() - t0
        print(f"    {D}Index built in {idx_t:.2f}s{Z}")
        results["insert"]["index_s"] = idx_t

    # MySQL InnoDB FULLTEXT needs explicit cache flush
    if hasattr(engine, 'conn') and engine.name == "MySQL":
        cur = engine.conn.cursor()
        cur.execute("SET GLOBAL innodb_optimize_fulltext_only=ON")
        cur.execute("OPTIMIZE TABLE articles")
        cur.execute("SET GLOBAL innodb_optimize_fulltext_only=OFF")
        cur.close()
        # Verify FT index is populated
        cur = engine.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles WHERE MATCH(text_) AGAINST('+democracy' IN BOOLEAN MODE)")
        ft_check = cur.fetchone()[0]
        cur.close()
        if ft_check == 0:
            print(f"    {Y}Warning: FULLTEXT index not yet populated, waiting...{Z}")
            time.sleep(2)

    # ── GET ──
    print(f"\n  {B}GET (point lookups){Z}")
    sample = random.sample(keys, min(len(keys), 10000))
    times = []; found = 0; t0 = time.perf_counter()
    for key in sample:
        bt = time.perf_counter()
        r = engine.get_one(key)
        times.append((time.perf_counter() - bt) * 1000)
        if r: found += 1
    total = time.perf_counter() - t0
    rate = len(sample) / total; med = median(times); p95 = pct(times, 95)
    print(f"    {G}{len(sample):,} lookups in {total:.2f}s{Z} \u2014 {B}{fmt_ops(rate)}/s{Z}  med={fmt_ms(med)}  p95={fmt_ms(p95)}  ({found} found)")
    results["get"] = {"ops_sec": rate, "median_ms": med, "p95_ms": p95}

    # ── SCAN ──
    print(f"\n  {B}Scan (page_size=100){Z}")
    times = []; rows = 0
    for p in range(20):
        bt = time.perf_counter()
        page = engine.scan_page(100, p * 100)
        times.append((time.perf_counter() - bt) * 1000)
        rows += len(page) if isinstance(page, (list, tuple)) else 0
        if isinstance(page, (list, tuple)) and len(page) < 100: break
    med = median(times)
    rate = rows / (sum(times)/1000) if sum(times) > 0 else 0
    print(f"    {rows:,} rows, {len(times)} pages \u2014 {B}{fmt_ops(rate)} rows/s{Z}  med={fmt_ms(med)}")
    results["scan"] = {"rows_sec": rate, "median_ms": med}

    # ── SEARCH FTS ──
    print(f"\n  {B}Full-text search (21 queries){Z}")
    stimes = []; total_hits = 0
    for q in QUERIES:
        bt = time.perf_counter()
        r = engine.search_fts(q, 20)
        stimes.append((time.perf_counter() - bt) * 1000)
        total_hits += len(r) if isinstance(r, list) else 0
    med = median(stimes); p95v = pct(stimes, 95)
    print(f"    {total_hits} hits \u2014 med={B}{fmt_ms(med)}{Z}  p95={fmt_ms(p95v)}")
    results["search_fts"] = {"median_ms": med, "p95_ms": p95v, "total_hits": total_hits}

    # ── SEARCH SUBSTR ──
    print(f"\n  {B}Substring search (21 queries){Z}")
    stimes = []; total_hits = 0
    for q in QUERIES:
        bt = time.perf_counter()
        r = engine.search_substr(q, 20)
        stimes.append((time.perf_counter() - bt) * 1000)
        total_hits += len(r) if isinstance(r, list) else 0
    med = median(stimes); p95v = pct(stimes, 95)
    print(f"    {total_hits} hits \u2014 med={B}{fmt_ms(med)}{Z}  p95={fmt_ms(p95v)}")
    results["search_substr"] = {"median_ms": med, "p95_ms": p95v, "total_hits": total_hits}

    # ── RECALL ──
    print(f"\n  {B}Recall (20 title searches){Z}")
    candidates = [(t, a) for t, a in docs if len(t.split()) >= 2 and t.isascii() and '"' not in t and "'" not in t]
    sample_docs = random.sample(candidates, min(20, len(candidates)))
    found_n = 0
    for title, _ in sample_docs:
        r = engine.search_fts(title, 50)
        if isinstance(r, list):
            for item in r:
                item_str = json.dumps(item) if isinstance(item, dict) else str(item)
                if title in item_str:
                    found_n += 1; break
        elif isinstance(r, (tuple, list)) and r:
            for item in r:
                if title in str(item):
                    found_n += 1; break
    recall = found_n / len(sample_docs) * 100
    color = G if recall >= 80 else (Y if recall >= 50 else R)
    print(f"    {color}{found_n}/{len(sample_docs)} ({recall:.0f}%){Z}")
    results["recall"] = {"pct": recall, "found": found_n, "total": len(sample_docs)}

    engine.teardown()
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=1995)
    ap.add_argument("--json", default="/tmp/wikipedia_search_bench.json")
    args = ap.parse_args()

    sep = "=" * 60
    print(f"\n{B}{sep}{Z}")
    print(f"{B}  Wikipedia Search Benchmark \u2014 4 Engines Head-to-Head{Z}")
    print(f"{B}  {D}{args.docs:,} articles | TurboDB vs PostgreSQL vs MongoDB vs MySQL{Z}")
    print(f"{B}{sep}{Z}")

    docs = load_articles(args.docs)
    keys = [t.replace("/", "_").replace(" ", "_")[:128].encode("ascii", "ignore").decode() for t, _ in docs]
    print(f"  {G}{len(docs):,} articles loaded{Z}")

    # Use same random seed for fair comparison
    random.seed(42)

    engines = [TurboDBEngine(), PostgresEngine(), MongoEngine(), MySQLEngine()]
    all_results = {}

    for eng in engines:
        random.seed(42)  # reset for each engine
        try:
            all_results[eng.name] = run_benchmark(eng, docs, keys)
        except Exception as e:
            print(f"\n  {R}{eng.name} FAILED: {e}{Z}")
            all_results[eng.name] = {"error": str(e)}

    # ── Final comparison table ──
    print(f"\n{B}{sep}{Z}")
    print(f"{B}  COMPARISON TABLE \u2014 {len(docs):,} Wikipedia articles{Z}")
    print(f"{B}{sep}{Z}")

    header = f"  {'Engine':<12} {'Insert/s':>10} {'GET/s':>10} {'FTS med':>10} {'Substr med':>12} {'Recall':>8}"
    print(f"\n{header}")
    print(f"  {'\u2500'*12} {'\u2500'*10} {'\u2500'*10} {'\u2500'*10} {'\u2500'*12} {'\u2500'*8}")

    for name, r in all_results.items():
        if "error" in r:
            print(f"  {name:<12} {R}ERROR{Z}")
            continue
        ins = fmt_ops(r["insert"]["ops_sec"])
        get = fmt_ops(r["get"]["ops_sec"])
        fts = fmt_ms(r["search_fts"]["median_ms"])
        sub = fmt_ms(r["search_substr"]["median_ms"])
        rec = f"{r['recall']['pct']:.0f}%"
        print(f"  {name:<12} {ins:>10} {get:>10} {fts:>10} {sub:>12} {rec:>8}")

    # JSON output
    report = {"dataset": "wikipedia_simple_english", "doc_count": len(docs),
              "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"), "engines": all_results}
    with open(args.json, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n  {D}Results \u2192 {args.json}{Z}")


if __name__ == "__main__":
    main()
