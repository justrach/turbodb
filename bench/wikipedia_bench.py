#!/usr/bin/env python3
"""
TurboDB Wikipedia Search Benchmark (FFI — no HTTP)
====================================================
Downloads Wikipedia articles (Simple English) from HuggingFace, loads them
into TurboDB via the native FFI (ctypes → libturbodb), and benchmarks:
  1. Bulk insert throughput
  2. Trigram search (full-text substring matching)
  3. Point lookups (GET by key)
  4. Scan performance
  5. Search recall quality

Usage:
  python3 bench/wikipedia_bench.py [--docs N]

Requires: libturbodb.dylib at ./zig-out/lib/ (run `zig build` first)
Data: Streams from HuggingFace API, cached at /tmp/turbodb_wiki_data.jsonl
"""

import json, os, sys, time
import shutil, argparse, random, re
from statistics import median

# Add project root to path so we can import turbodb
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
from turbodb import Database

# ── Colors ────────────────────────────────────────────────────────────────────
B = "\033[1m"; G = "\033[32m"; C = "\033[36m"; D = "\033[2m"
Y = "\033[33m"; R = "\033[31m"; M = "\033[35m"; Z = "\033[0m"

CACHE_PATH = "/tmp/turbodb_wiki_data.jsonl"

# ── Wikipedia Data ────────────────────────────────────────────────────────────

def load_wiki_articles(max_docs=100_000):
    """Load Wikipedia articles — from cache or stream from HuggingFace API."""
    if os.path.exists(CACHE_PATH):
        sz = os.path.getsize(CACHE_PATH) / (1024 * 1024)
        print(f"  {D}Using cached data ({sz:.1f} MB): {CACHE_PATH}{Z}")
        docs = []
        with open(CACHE_PATH, "r") as f:
            for line in f:
                if len(docs) >= max_docs:
                    break
                obj = json.loads(line)
                docs.append((obj["title"], obj["text"]))
        return docs

    # Stream from HuggingFace API and cache
    from urllib.request import Request, urlopen
    from urllib.parse import quote

    HF_API = "https://datasets-server.huggingface.co/rows"
    DATASET = "wikimedia/wikipedia"
    CONFIG = "20231101.simple"  # Simple English — fast, ~200K articles
    BATCH = 100

    docs = []
    cache_f = open(CACHE_PATH, "w")
    offset = 0

    print(f"  {C}Streaming from HuggingFace API (Simple English Wikipedia)...{Z}")

    while len(docs) < max_docs:
        remaining = min(BATCH, max_docs - len(docs))
        url = f"{HF_API}?dataset={quote(DATASET)}&config={CONFIG}&split=train&offset={offset}&length={remaining}"
        req = Request(url, headers={"User-Agent": "TurboDB-Bench/1.0"})

        try:
            data = json.loads(urlopen(req, timeout=30).read())
        except Exception as e:
            print(f"\n  {Y}API error at offset {offset}: {e}{Z}")
            break

        rows = data.get("rows", [])
        if not rows:
            break

        for row in rows:
            r = row["row"]
            title = r.get("title", "")
            text = r.get("text", "")
            abstract = text[:1500] if text else ""
            if title and len(abstract) > 50:
                cache_f.write(json.dumps({"title": title, "text": abstract}) + "\n")
                docs.append((title, abstract))

        offset += len(rows)
        if len(docs) % 1000 == 0:
            print(f"\r  {D}Fetched {len(docs):,}/{max_docs:,} articles...{Z}", end="", flush=True)

    cache_f.close()
    print(f"\r  {G}Fetched {len(docs):,} articles{Z}                    ")
    return docs


# ── Benchmark Helpers ─────────────────────────────────────────────────────────

def fmt_ops(ops):
    if ops >= 1_000_000: return f"{ops/1_000_000:.1f}M"
    if ops >= 1_000: return f"{ops/1_000:.1f}K"
    return f"{ops:.0f}"

def fmt_ms(ms):
    if ms < 0.001: return f"{ms*1_000_000:.0f}ns"
    if ms < 1: return f"{ms*1000:.0f}µs"
    if ms < 1000: return f"{ms:.1f}ms"
    return f"{ms/1000:.2f}s"

def percentile(arr, pct):
    s = sorted(arr)
    idx = int(len(s) * pct / 100)
    return s[min(idx, len(s) - 1)]


# ── Benchmarks ────────────────────────────────────────────────────────────────

def bench_insert(db, docs, col_name="wiki"):
    """Benchmark bulk insert throughput via FFI."""
    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  BENCHMARK 1: Bulk Insert — FFI ({len(docs):,} documents){Z}")
    print(f"{B}{'='*60}{Z}")

    col = db.collection(col_name)
    t0 = time.perf_counter()
    times = []
    inserted = 0

    for title, abstract in docs:
        key = title.replace("/", "_").replace(" ", "_")[:128]
        val = json.dumps({"title": title, "text": abstract, "length": len(abstract)})

        bt0 = time.perf_counter()
        try:
            col.insert(key, val)
            inserted += 1
        except Exception:
            pass  # skip duplicates
        times.append((time.perf_counter() - bt0) * 1000)

        if inserted % 5000 == 0 and inserted > 0:
            elapsed = time.perf_counter() - t0
            rate = inserted / elapsed
            print(f"\r  {D}Inserted {inserted:,}/{len(docs):,} ({rate:,.0f} docs/s)...{Z}", end="", flush=True)

    elapsed = time.perf_counter() - t0
    rate = inserted / elapsed
    med = median(times)
    p95 = percentile(times, 95)
    p99 = percentile(times, 99)

    print(f"\r  {G}✓ Inserted {inserted:,} docs in {elapsed:.2f}s{Z}              ")
    print(f"  {C}Throughput:{Z} {B}{fmt_ops(rate)} docs/sec{Z}")
    print(f"  {C}Latency:{Z}   median={fmt_ms(med)}  p95={fmt_ms(p95)}  p99={fmt_ms(p99)}")

    return {"ops": inserted, "elapsed_s": elapsed, "ops_per_sec": rate,
            "median_ms": med, "p95_ms": p95, "p99_ms": p99}


def bench_get(db, keys, col_name="wiki"):
    """Benchmark point lookups via FFI."""
    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  BENCHMARK 2: Point Lookups — FFI (GET by key){Z}")
    print(f"{B}{'='*60}{Z}")

    col = db.collection(col_name)
    sample_size = min(50_000, len(keys))
    sample = random.sample(keys, sample_size)

    times = []
    found = 0
    t0 = time.perf_counter()

    for key in sample:
        bt0 = time.perf_counter()
        result = col.get(key)
        times.append((time.perf_counter() - bt0) * 1000)
        if result is not None:
            found += 1

    total = time.perf_counter() - t0
    rate = sample_size / total
    med = median(times)
    p95 = percentile(times, 95)
    p99 = percentile(times, 99)

    print(f"  {G}✓ {sample_size:,} lookups in {total:.2f}s{Z}  ({found:,} found)")
    print(f"  {C}Throughput:{Z} {B}{fmt_ops(rate)} gets/sec{Z}")
    print(f"  {C}Latency:{Z}   median={fmt_ms(med)}  p95={fmt_ms(p95)}  p99={fmt_ms(p99)}")

    return {"ops": sample_size, "elapsed_s": total, "ops_per_sec": rate,
            "found": found, "median_ms": med, "p95_ms": p95, "p99_ms": p99}


def bench_scan(db, col_name="wiki"):
    """Benchmark scan (pagination) via FFI."""
    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  BENCHMARK 3: Scan — FFI (Pagination){Z}")
    print(f"{B}{'='*60}{Z}")

    col = db.collection(col_name)
    page_sizes = [20, 100, 500, 1000]
    results = []

    for ps in page_sizes:
        pages = max(1, 5000 // ps)
        times = []
        total_rows = 0

        for p in range(pages):
            bt0 = time.perf_counter()
            docs = col.scan(limit=ps, offset=p * ps)
            times.append((time.perf_counter() - bt0) * 1000)
            total_rows += len(docs)
            if len(docs) < ps:
                break

        med = median(times)
        total_ms = sum(times)
        rate = total_rows / (total_ms / 1000) if total_ms > 0 else 0

        print(f"  page_size={ps:<5}  pages={len(times):<4}  rows={total_rows:<6}  "
              f"median={fmt_ms(med):>8}  {B}{fmt_ops(rate):>8} rows/s{Z}")

        results.append({"page_size": ps, "pages": len(times), "rows": total_rows,
                        "median_ms": med, "rows_per_sec": rate})

    return results



def bench_insert_http(port, docs, col_name="wiki"):
    """Benchmark insert throughput via HTTP (builds trigram index in server)."""
    import http.client

    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  BENCHMARK: Bulk Insert — HTTP ({len(docs):,} documents){Z}")
    print(f"{B}{'='*60}{Z}")
    print(f"  {D}(Inserts via HTTP so trigram index is built server-side){Z}")

    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    t0 = time.perf_counter()
    times = []
    inserted = 0

    for title, abstract in docs:
        key = title.replace("/", "_").replace(" ", "_")[:128]
        val = json.dumps({"title": title, "text": abstract, "length": len(abstract)})
        body = json.dumps({"key": key, "value": val}).encode()

        bt0 = time.perf_counter()
        try:
            conn.request("POST", f"/db/{col_name}", body=body,
                         headers={"Content-Type": "application/json",
                                  "Content-Length": str(len(body)),
                                  "Connection": "keep-alive"})
            resp = conn.getresponse()
            resp.read()
            inserted += 1
        except Exception:
            try: conn.close()
            except: pass
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        times.append((time.perf_counter() - bt0) * 1000)

        if inserted % 500 == 0 and inserted > 0:
            elapsed = time.perf_counter() - t0
            rate = inserted / elapsed
            print(f"\r  {D}Inserted {inserted:,}/{len(docs):,} ({rate:,.0f} docs/s)...{Z}", end="", flush=True)

    conn.close()
    elapsed = time.perf_counter() - t0
    rate = inserted / elapsed
    med = median(times)
    p95 = percentile(times, 95)
    p99 = percentile(times, 99)

    print(f"\r  {G}✓ Inserted {inserted:,} docs in {elapsed:.2f}s{Z}              ")
    print(f"  {C}Throughput:{Z} {B}{fmt_ops(rate)} docs/sec{Z}")
    print(f"  {C}Latency:{Z}   median={fmt_ms(med)}  p95={fmt_ms(p95)}  p99={fmt_ms(p99)}")

    return {"ops": inserted, "elapsed_s": elapsed, "ops_per_sec": rate,
            "median_ms": med, "p95_ms": p95, "p99_ms": p99}

def bench_search_ffi(db, col_name="wiki"):
    """Benchmark trigram search via FFI (in-process, no HTTP)."""
    sep = "=" * 60
    print(f"\n{B}{sep}{Z}")
    print(f"{B}  BENCHMARK: Full-Text Search - FFI (Trigram Index){Z}")
    print(f"{B}{sep}{Z}")
    col = db.collection(col_name)
    queries = [
        "United States", "quantum physics", "machine learning",
        "solar system", "world war", "climate change",
        "photosynthesis", "democracy", "chromosome",
        "algorithm", "electromagnetic", "biodiversity",
        "artificial intelligence", "human rights",
        "carbon dioxide", "natural selection",
        "mitochondria", "fibonacci", "superconductor",
        "paleontology", "cryptography",
    ]
    results = []
    total_hits = 0
    for q in queries:
        bt0 = time.perf_counter()
        docs = col.search(q, limit=20)
        elapsed_ms = (time.perf_counter() - bt0) * 1000
        hits = len(docs)
        results.append({"query": q, "hits": hits, "elapsed_ms": elapsed_ms})
        total_hits += hits
    search_times = [r["elapsed_ms"] for r in results]
    avg_ms = sum(search_times) / len(search_times)
    med_ms = median(search_times)
    p95_ms = percentile(search_times, 95)
    print()
    for r in results:
        q, h, t = r["query"], r["hits"], fmt_ms(r["elapsed_ms"])
        print(f"  {q:<25} hits={h:>3}  {t:>8}")
    print(f"\n  {C}Summary:{Z} {len(queries)} queries, {total_hits} total hits")
    print(f"  {C}Latency:{Z} avg={fmt_ms(avg_ms)}  median={B}{fmt_ms(med_ms)}{Z}  p95={fmt_ms(p95_ms)}")
    return {"queries": len(queries), "total_hits": total_hits,
            "avg_ms": avg_ms, "median_ms": med_ms, "p95_ms": p95_ms,
            "details": results}


def bench_search_http(port, col_name="wiki"):
    """Benchmark trigram search — requires HTTP server (search not yet in FFI scan)."""
    import http.client
    from urllib.parse import quote

    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  BENCHMARK 4: Full-Text Search — Trigram Index (HTTP){Z}")
    print(f"{B}{'='*60}{Z}")
    print(f"  {D}(Search endpoint is HTTP-only — testing via localhost:{port}){Z}")

    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)

    queries = [
        "United States", "quantum physics", "machine learning",
        "solar system", "world war", "climate change",
        "photosynthesis", "democracy", "chromosome",
        "algorithm", "electromagnetic", "biodiversity",
        "artificial intelligence", "human rights",
        "carbon dioxide", "natural selection",
        "mitochondria", "fibonacci", "superconductor",
        "paleontology", "cryptography",
    ]

    results = []
    total_hits = 0

    for q in queries:
        path = f"/search/{col_name}?q={quote(q)}&limit=20"
        bt0 = time.perf_counter()
        conn.request("GET", path, headers={"Connection": "keep-alive"})
        resp = conn.getresponse()
        data = json.loads(resp.read())
        elapsed_ms = (time.perf_counter() - bt0) * 1000

        hits = data.get("hits", 0)
        candidates = data.get("candidates", 0)
        total_docs = data.get("total_docs", 0)
        selectivity = (candidates / total_docs * 100) if total_docs else 0

        results.append({
            "query": q, "hits": hits, "candidates": candidates,
            "selectivity": selectivity, "elapsed_ms": elapsed_ms,
        })
        total_hits += hits

    conn.close()

    # Print results table
    print(f"\n  {'Query':<25} {'Hits':>5} {'Cands':>6} {'Select':>7} {'Time':>8}")
    print(f"  {'─'*25} {'─'*5} {'─'*6} {'─'*7} {'─'*8}")

    for r in results:
        sel_color = G if r["selectivity"] < 10 else (Y if r["selectivity"] < 50 else R)
        print(f"  {r['query']:<25} {r['hits']:>5} {r['candidates']:>6} "
              f"{sel_color}{r['selectivity']:>5.1f}%{Z} {fmt_ms(r['elapsed_ms']):>8}")

    search_times = [r["elapsed_ms"] for r in results]
    avg_ms = sum(search_times) / len(search_times)
    med_ms = median(search_times)
    p95_ms = percentile(search_times, 95)

    print(f"\n  {C}Summary:{Z} {len(queries)} queries, {total_hits} total hits")
    print(f"  {C}Latency:{Z} avg={fmt_ms(avg_ms)}  median={B}{fmt_ms(med_ms)}{Z}  p95={fmt_ms(p95_ms)}")

    return {"queries": len(queries), "total_hits": total_hits,
            "avg_ms": avg_ms, "median_ms": med_ms, "p95_ms": p95_ms,
            "details": results}


def bench_search_quality(port, docs, col_name="wiki"):
    """Test search recall — search by article title and verify it's in results."""
    import http.client
    from urllib.parse import quote

    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  BENCHMARK 5: Search Recall Quality{Z}")
    print(f"{B}{'='*60}{Z}")

    # Pick random articles with clean ASCII titles (avoid encoding issues)
    candidates = [(t, a) for t, a in docs
                  if len(t.split()) >= 2 and len(t) >= 8
                  and t.isascii() and '"' not in t and "'" not in t]
    sample = random.sample(candidates, min(20, len(candidates)))
    tests = []

    for title, abstract in sample:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        path = f"/search/{col_name}?q={quote(title)}&limit=50"
        try:
            bt0 = time.perf_counter()
            conn.request("GET", path, headers={"Connection": "close"})
            resp = conn.getresponse()
            data = json.loads(resp.read())
            elapsed_ms = (time.perf_counter() - bt0) * 1000

            hits = data.get("hits", 0)
            found = False
            for r in data.get("results", []):
                val = r.get("value", "")
                # value may be a dict (parsed JSON) or a string
                val_str = json.dumps(val) if isinstance(val, dict) else str(val)
                if title in val_str:
                    found = True
                    break

            tests.append({"query": title[:40], "hits": hits,
                           "found": found, "time_ms": elapsed_ms})
        except Exception as e:
            tests.append({"query": title[:40], "hits": 0,
                           "found": False, "time_ms": 0})
        finally:
            conn.close()

    found_count = sum(1 for t in tests if t["found"])
    total_tests = len(tests)
    recall = found_count / total_tests * 100 if total_tests else 0

    for t in tests:
        icon = f"{G}✓{Z}" if t["found"] else f"{R}✗{Z}"
        print(f"  {icon} \"{t['query']:<40}\"  hits={t['hits']:<3}  {fmt_ms(t['time_ms'])}")

    color = G if recall >= 80 else (Y if recall >= 50 else R)
    print(f"\n  {C}Recall:{Z} {color}{found_count}/{total_tests} ({recall:.0f}%){Z}")

    return {"tests": total_tests, "found": found_count, "recall_pct": recall}

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import subprocess
    import http.client

    ap = argparse.ArgumentParser(description="TurboDB Wikipedia Search Benchmark")
    ap.add_argument("--docs", type=int, default=50_000, help="Number of docs to load (default: 50K)")
    ap.add_argument("--port", type=int, default=27030, help="HTTP port for search benchmarks")
    ap.add_argument("--binary", default="./zig-out/bin/turbodb")
    ap.add_argument("--json", default="/tmp/turbodb_wiki_bench.json", help="JSON output path")
    args = ap.parse_args()

    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  TurboDB Wikipedia Search Benchmark{Z}")
    print(f"{B}  {D}FFI (in-process) + HTTP (search) | {args.docs:,} docs{Z}")
    print(f"{B}{'='*60}{Z}")

    # 1. Load Wikipedia data
    print(f"\n{B}Phase 1: Loading Wikipedia articles{Z}")
    t0 = time.time()
    docs = load_wiki_articles(max_docs=args.docs)
    elapsed = time.time() - t0
    print(f"  {G}{len(docs):,} articles loaded in {elapsed:.1f}s{Z}")

    if docs:
        t, a = docs[0]
        print(f"  {D}Sample: \"{t}\" → \"{a[:80]}...\"{Z}")

    keys = [title.replace("/", "_").replace(" ", "_")[:128] for title, _ in docs]

    # ── Phase 2: FFI benchmarks (raw engine performance, no server) ──
    print(f"\n{B}Phase 2: FFI Benchmarks (in-process, no network){Z}")
    data_dir_ffi = "/tmp/turbodb_wiki_ffi_data"
    if os.path.exists(data_dir_ffi):
        shutil.rmtree(data_dir_ffi)
    os.makedirs(data_dir_ffi, exist_ok=True)

    db = Database(data_dir_ffi)
    r_insert_ffi = bench_insert(db, docs)
    r_get_ffi = bench_get(db, keys)
    r_scan_ffi = bench_scan(db)
    r_search_ffi = bench_search_ffi(db)
    db.close()

    # ── Phase 3: HTTP server benchmarks (trigram search needs server) ──
    print(f"\n{B}Phase 3: HTTP Server Benchmarks (search + insert){Z}")
    data_dir_http = "/tmp/turbodb_wiki_http_data"
    if os.path.exists(data_dir_http):
        shutil.rmtree(data_dir_http)
    os.makedirs(data_dir_http, exist_ok=True)

    subprocess.run(["pkill", "-f", f"turbodb.*{args.port}"], capture_output=True, timeout=3)
    time.sleep(0.3)

    proc = subprocess.Popen(
        [args.binary, "--data", data_dir_http, "--port", str(args.port), "--http"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )

    # Wait for server
    for _ in range(60):
        time.sleep(0.1)
        try:
            c = http.client.HTTPConnection("127.0.0.1", args.port, timeout=2)
            c.request("GET", "/health")
            if json.loads(c.getresponse().read()).get("engine") == "TurboDB":
                c.close()
                break
            c.close()
        except Exception:
            pass
    else:
        print(f"  {R}Failed to start HTTP server{Z}")
        proc.terminate()
        sys.exit(1)
    print(f"  {G}HTTP server ready on :{args.port} (pid {proc.pid}){Z}")

    try:
        # Insert via HTTP so trigram index is built in the server process
        r_insert_http = bench_insert_http(args.port, docs)
        r_search = bench_search_http(args.port)
        r_quality = bench_search_quality(args.port, docs)
    finally:
        proc.kill()
        try: proc.wait(timeout=3)
        except: pass

    # ── Summary ──
    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  SUMMARY — {len(docs):,} Wikipedia articles{Z}")
    print(f"{B}{'='*60}{Z}")
    print(f"\n  {M}FFI (in-process, no network):{Z}")
    print(f"  {C}Insert:{Z}  {B}{fmt_ops(r_insert_ffi['ops_per_sec'])} docs/sec{Z}  median {fmt_ms(r_insert_ffi['median_ms'])}")
    print(f"  {C}GET:{Z}     {B}{fmt_ops(r_get_ffi['ops_per_sec'])} gets/sec{Z}  median {fmt_ms(r_get_ffi['median_ms'])}")
    print(f"  {C}Search:{Z}  median {B}{fmt_ms(r_search_ffi["median_ms"])}{Z}  ({r_search_ffi["total_hits"]} hits / {r_search_ffi["queries"]} queries)")
    print(f"\n  {M}HTTP (server, includes trigram indexing):{Z}")
    print(f"  {C}Insert:{Z}  {B}{fmt_ops(r_insert_http['ops_per_sec'])} docs/sec{Z}  median {fmt_ms(r_insert_http['median_ms'])}")
    print(f"  {C}Search:{Z}  median {B}{fmt_ms(r_search['median_ms'])}{Z}  ({r_search['total_hits']} hits / {r_search['queries']} queries)")
    print(f"  {C}Recall:{Z}  {r_quality['recall_pct']:.0f}% ({r_quality['found']}/{r_quality['tests']})")

    # Write JSON
    report = {
        "engine": "TurboDB",
        "dataset": "wikipedia_simple_english",
        "doc_count": len(docs),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "ffi": {"insert": r_insert_ffi, "get": r_get_ffi, "scan": r_scan_ffi, "search": r_search_ffi},
        "http": {"insert": r_insert_http, "search": r_search, "quality": r_quality},
    }
    with open(args.json, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n  {D}Results → {args.json}{Z}")
if __name__ == "__main__":
    main()
