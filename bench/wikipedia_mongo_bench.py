#!/usr/bin/env python3
"""
MongoDB Wikipedia Benchmark — same workload as wikipedia_bench.py
=================================================================
Runs the identical Wikipedia dataset through MongoDB for apples-to-apples
comparison with TurboDB.

Usage:
  python3.13 bench/wikipedia_mongo_bench.py [--docs N]

Requires: pymongo, mongod running on localhost:27017
"""

import json, os, sys, time, random, re, argparse
from statistics import median

import pymongo

B = "\033[1m"; G = "\033[32m"; C = "\033[36m"; D = "\033[2m"
Y = "\033[33m"; R = "\033[31m"; M = "\033[35m"; Z = "\033[0m"

CACHE_PATH = "/tmp/turbodb_wiki_data.jsonl"

def load_wiki_articles(max_docs=100_000):
    if not os.path.exists(CACHE_PATH):
        print(f"  {R}No cached data at {CACHE_PATH}. Run wikipedia_bench.py first.{Z}")
        sys.exit(1)
    docs = []
    with open(CACHE_PATH, "r") as f:
        for line in f:
            if len(docs) >= max_docs: break
            obj = json.loads(line)
            docs.append((obj["title"], obj["text"]))
    return docs

def fmt_ops(ops):
    if ops >= 1_000_000: return f"{ops/1_000_000:.1f}M"
    if ops >= 1_000: return f"{ops/1_000:.1f}K"
    return f"{ops:.0f}"

def fmt_ms(ms):
    if ms < 0.001: return f"{ms*1_000_000:.0f}ns"
    if ms < 1: return f"{ms*1000:.0f}\u00b5s"
    if ms < 1000: return f"{ms:.1f}ms"
    return f"{ms/1000:.2f}s"

def percentile(arr, pct):
    s = sorted(arr)
    return s[min(int(len(s) * pct / 100), len(s) - 1)]

def bench_insert(col, docs):
    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  BENCHMARK 1: Bulk Insert ({len(docs):,} documents){Z}")
    print(f"{B}{'='*60}{Z}")
    col.drop()
    t0 = time.perf_counter(); times = []; inserted = 0
    for title, abstract in docs:
        key = title.replace("/", "_").replace(" ", "_")[:128]
        doc = {"_id": key, "title": title, "text": abstract, "length": len(abstract)}
        bt0 = time.perf_counter()
        try: col.insert_one(doc); inserted += 1
        except pymongo.errors.DuplicateKeyError: pass
        times.append((time.perf_counter() - bt0) * 1000)
        if inserted % 500 == 0 and inserted > 0:
            elapsed = time.perf_counter() - t0
            print(f"\r  {D}Inserted {inserted:,}/{len(docs):,} ({inserted/elapsed:,.0f} docs/s)...{Z}", end="", flush=True)
    elapsed = time.perf_counter() - t0
    rate = inserted / elapsed
    med = median(times); p95 = percentile(times, 95); p99 = percentile(times, 99)
    print(f"\r  {G}\u2713 Inserted {inserted:,} docs in {elapsed:.2f}s{Z}              ")
    print(f"  {C}Throughput:{Z} {B}{fmt_ops(rate)} docs/sec{Z}")
    print(f"  {C}Latency:{Z}   median={fmt_ms(med)}  p95={fmt_ms(p95)}  p99={fmt_ms(p99)}")
    return {"ops": inserted, "elapsed_s": elapsed, "ops_per_sec": rate, "median_ms": med, "p95_ms": p95, "p99_ms": p99}

def bench_insert_with_index(col, docs):
    result = bench_insert(col, docs)
    print(f"  {D}Creating text index on 'text' field...{Z}", end=" ", flush=True)
    t0 = time.perf_counter()
    col.create_index([("text", "text")])
    idx_time = time.perf_counter() - t0
    print(f"{G}done ({idx_time:.2f}s){Z}")
    result["index_time_s"] = idx_time
    return result

def bench_get(col, keys):
    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  BENCHMARK 2: Point Lookups (find_one by _id){Z}")
    print(f"{B}{'='*60}{Z}")
    sample_size = min(50_000, len(keys))
    sample = random.sample(keys, sample_size)
    times = []; found = 0; t0 = time.perf_counter()
    for key in sample:
        bt0 = time.perf_counter()
        result = col.find_one({"_id": key})
        times.append((time.perf_counter() - bt0) * 1000)
        if result is not None: found += 1
    total = time.perf_counter() - t0
    rate = sample_size / total
    med = median(times); p95 = percentile(times, 95); p99 = percentile(times, 99)
    print(f"  {G}\u2713 {sample_size:,} lookups in {total:.2f}s{Z}  ({found:,} found)")
    print(f"  {C}Throughput:{Z} {B}{fmt_ops(rate)} gets/sec{Z}")
    print(f"  {C}Latency:{Z}   median={fmt_ms(med)}  p95={fmt_ms(p95)}  p99={fmt_ms(p99)}")
    return {"ops": sample_size, "elapsed_s": total, "ops_per_sec": rate, "found": found, "median_ms": med, "p95_ms": p95, "p99_ms": p99}

def bench_scan(col):
    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  BENCHMARK 3: Scan (Pagination){Z}")
    print(f"{B}{'='*60}{Z}")
    page_sizes = [20, 100, 500, 1000]; results = []
    for ps in page_sizes:
        pages = max(1, 5000 // ps); times = []; total_rows = 0
        for p in range(pages):
            bt0 = time.perf_counter()
            docs = list(col.find().skip(p * ps).limit(ps))
            times.append((time.perf_counter() - bt0) * 1000)
            total_rows += len(docs)
            if len(docs) < ps: break
        med = median(times); total_ms = sum(times)
        rate = total_rows / (total_ms / 1000) if total_ms > 0 else 0
        print(f"  page_size={ps:<5}  pages={len(times):<4}  rows={total_rows:<6}  median={fmt_ms(med):>8}  {B}{fmt_ops(rate):>8} rows/s{Z}")
        results.append({"page_size": ps, "pages": len(times), "rows": total_rows, "median_ms": med, "rows_per_sec": rate})
    return results

def bench_search(col):
    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  BENCHMARK 4: Full-Text Search ($text index){Z}")
    print(f"{B}{'='*60}{Z}")
    queries = ["United States", "quantum physics", "machine learning", "solar system", "world war", "climate change",
               "photosynthesis", "democracy", "chromosome", "algorithm", "electromagnetic", "biodiversity",
               "artificial intelligence", "human rights", "carbon dioxide", "natural selection",
               "mitochondria", "fibonacci", "superconductor", "paleontology", "cryptography"]
    results = []; total_hits = 0
    for q in queries:
        bt0 = time.perf_counter()
        docs = list(col.find({"$text": {"$search": q}}).limit(20))
        elapsed_ms = (time.perf_counter() - bt0) * 1000
        results.append({"query": q, "hits": len(docs), "elapsed_ms": elapsed_ms})
        total_hits += len(docs)
    print(f"\n  {'Query':<25} {'Hits':>5} {'Time':>8}")
    print(f"  {'\u2500'*25} {'\u2500'*5} {'\u2500'*8}")
    for r in results:
        print(f"  {r['query']:<25} {r['hits']:>5} {fmt_ms(r['elapsed_ms']):>8}")
    search_times = [r["elapsed_ms"] for r in results]
    avg_ms = sum(search_times) / len(search_times); med_ms = median(search_times); p95_ms = percentile(search_times, 95)
    print(f"\n  {C}Summary:{Z} {len(queries)} queries, {total_hits} total hits")
    print(f"  {C}Latency:{Z} avg={fmt_ms(avg_ms)}  median={B}{fmt_ms(med_ms)}{Z}  p95={fmt_ms(p95_ms)}")
    return {"queries": len(queries), "total_hits": total_hits, "avg_ms": avg_ms, "median_ms": med_ms, "p95_ms": p95_ms, "details": results}

def bench_search_regex(col):
    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  BENCHMARK 4b: Regex Search ($regex \u2014 substring match){Z}")
    print(f"{B}{'='*60}{Z}")
    queries = ["United States", "quantum physics", "machine learning", "solar system", "world war", "climate change",
               "photosynthesis", "democracy", "chromosome", "algorithm", "electromagnetic", "biodiversity",
               "artificial intelligence", "human rights", "carbon dioxide", "natural selection",
               "mitochondria", "fibonacci", "superconductor", "paleontology", "cryptography"]
    results = []; total_hits = 0
    for q in queries:
        escaped = re.escape(q)
        bt0 = time.perf_counter()
        docs = list(col.find({"text": {"$regex": escaped, "$options": "i"}}).limit(20))
        elapsed_ms = (time.perf_counter() - bt0) * 1000
        results.append({"query": q, "hits": len(docs), "elapsed_ms": elapsed_ms})
        total_hits += len(docs)
    print(f"\n  {'Query':<25} {'Hits':>5} {'Time':>8}")
    print(f"  {'\u2500'*25} {'\u2500'*5} {'\u2500'*8}")
    for r in results:
        print(f"  {r['query']:<25} {r['hits']:>5} {fmt_ms(r['elapsed_ms']):>8}")
    search_times = [r["elapsed_ms"] for r in results]
    avg_ms = sum(search_times) / len(search_times); med_ms = median(search_times); p95_ms = percentile(search_times, 95)
    print(f"\n  {C}Summary:{Z} {len(queries)} queries, {total_hits} total hits")
    print(f"  {C}Latency:{Z} avg={fmt_ms(avg_ms)}  median={B}{fmt_ms(med_ms)}{Z}  p95={fmt_ms(p95_ms)}")
    return {"queries": len(queries), "total_hits": total_hits, "avg_ms": avg_ms, "median_ms": med_ms, "p95_ms": p95_ms, "details": results}

def bench_recall(col, docs):
    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  BENCHMARK 5: Search Recall Quality{Z}")
    print(f"{B}{'='*60}{Z}")
    candidates = [(t, a) for t, a in docs if len(t.split()) >= 2 and len(t) >= 8 and t.isascii() and '"' not in t and "'" not in t]
    sample = random.sample(candidates, min(20, len(candidates))); tests = []
    for title, abstract in sample:
        bt0 = time.perf_counter()
        results = list(col.find({"$text": {"$search": f'"{title}"'}}).limit(50))
        elapsed_ms = (time.perf_counter() - bt0) * 1000
        found = any(title in r.get("title", "") for r in results)
        tests.append({"query": title[:40], "hits": len(results), "found": found, "time_ms": elapsed_ms})
    found_count = sum(1 for t in tests if t["found"])
    total_tests = len(tests)
    recall = found_count / total_tests * 100 if total_tests else 0
    for t in tests:
        icon = f"{G}\u2713{Z}" if t["found"] else f"{R}\u2717{Z}"
        print(f"  {icon} \"{t['query']:<40}\"  hits={t['hits']:<3}  {fmt_ms(t['time_ms'])}")
    color = G if recall >= 80 else (Y if recall >= 50 else R)
    print(f"\n  {C}Recall:{Z} {color}{found_count}/{total_tests} ({recall:.0f}%){Z}")
    return {"tests": total_tests, "found": found_count, "recall_pct": recall}

def main():
    ap = argparse.ArgumentParser(description="MongoDB Wikipedia Search Benchmark")
    ap.add_argument("--docs", type=int, default=50_000)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=27017)
    ap.add_argument("--json", default="/tmp/mongodb_wiki_bench.json")
    args = ap.parse_args()

    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  MongoDB Wikipedia Search Benchmark{Z}")
    print(f"{B}  {D}MongoDB 8.x | {args.docs:,} docs | {args.host}:{args.port}{Z}")
    print(f"{B}{'='*60}{Z}")

    print(f"\n{B}Phase 1: Loading Wikipedia articles{Z}")
    docs = load_wiki_articles(max_docs=args.docs)
    print(f"  {G}{len(docs):,} articles loaded{Z}")

    print(f"\n{B}Phase 2: Connecting to MongoDB{Z}")
    client = pymongo.MongoClient(args.host, args.port, serverSelectionTimeoutMS=5000)
    try: client.admin.command("ping")
    except Exception as e:
        print(f"  {R}Cannot connect: {e}{Z}"); sys.exit(1)
    db = client["wiki_bench"]; col = db["articles"]
    info = client.server_info()
    print(f"  {G}Connected to MongoDB {info.get('version', '?')}{Z}")

    keys = [title.replace("/", "_").replace(" ", "_")[:128] for title, _ in docs]

    r_insert = bench_insert_with_index(col, docs)
    r_get = bench_get(col, keys)
    r_scan = bench_scan(col)
    r_search_text = bench_search(col)
    r_search_regex = bench_search_regex(col)
    r_recall = bench_recall(col, docs)

    print(f"\n{B}{'='*60}{Z}")
    print(f"{B}  SUMMARY \u2014 MongoDB {info.get('version','?')} \u2014 {len(docs):,} articles{Z}")
    print(f"{B}{'='*60}{Z}")
    print(f"  {C}Insert:{Z}        {B}{fmt_ops(r_insert['ops_per_sec'])} docs/sec{Z}  median {fmt_ms(r_insert['median_ms'])}  (+{r_insert.get('index_time_s',0):.1f}s index)")
    print(f"  {C}GET:{Z}           {B}{fmt_ops(r_get['ops_per_sec'])} gets/sec{Z}  median {fmt_ms(r_get['median_ms'])}")
    print(f"  {C}Search ($text):{Z} median {B}{fmt_ms(r_search_text['median_ms'])}{Z}  ({r_search_text['total_hits']} hits)")
    print(f"  {C}Search (regex):{Z} median {B}{fmt_ms(r_search_regex['median_ms'])}{Z}  ({r_search_regex['total_hits']} hits)")
    print(f"  {C}Recall:{Z}        {r_recall['recall_pct']:.0f}% ({r_recall['found']}/{r_recall['tests']})")

    report = {"engine": f"MongoDB {info.get('version','?')}", "dataset": "wikipedia_simple_english",
              "doc_count": len(docs), "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
              "insert": r_insert, "get": r_get, "scan": r_scan,
              "search_text": r_search_text, "search_regex": r_search_regex, "recall": r_recall}
    with open(args.json, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n  {D}Results \u2192 {args.json}{Z}")
    col.drop(); client.close()

if __name__ == "__main__":
    main()
