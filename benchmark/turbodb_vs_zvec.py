#!/usr/bin/env python3
"""
TurboDB vs Zvec Benchmark
=========================
Compares vector search performance at scale:
  - TurboDB (local, INT8 SIMD parallel scan)
  - Zvec (alibaba/zvec, in turbobox sandbox)

Metrics: insert throughput, search latency, recall@10, memory usage.
Dimensions: 768, 1536. Scales: 10K, 50K, 100K vectors.
"""

import sys, os, time, json, random, math, ctypes, subprocess, argparse

# ─── TurboDB (local FFI) ─────────────────────────────────────────────────

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

def setup_turbodb_ffi():
    from turbodb import VectorColumn, _ffi
    for name, args, res in [
        ('turbodb_vector_enable_int8', [ctypes.c_void_p, ctypes.c_uint64], ctypes.c_int),
        ('turbodb_vector_search_int8', [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32, ctypes.c_uint32, ctypes.c_uint8, ctypes.POINTER(ctypes.c_uint32), ctypes.POINTER(ctypes.c_float)], ctypes.c_int),
        ('turbodb_vector_search_int8_parallel', [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32, ctypes.c_uint32, ctypes.c_uint8, ctypes.POINTER(ctypes.c_uint32), ctypes.POINTER(ctypes.c_float)], ctypes.c_int),
    ]:
        getattr(_ffi._lib, name).argtypes = args
        getattr(_ffi._lib, name).restype = res
    return VectorColumn, _ffi


def bench_turbodb(dims, n_vecs, queries, k=10):
    VectorColumn, _ffi = setup_turbodb_ffi()
    results = {"engine": "TurboDB", "dims": dims, "n_vecs": n_vecs}

    col = VectorColumn(dims)

    # Insert
    t0 = time.perf_counter()
    for v in generate_vecs(n_vecs, dims):
        col.append(v)
    results["insert_s"] = time.perf_counter() - t0
    results["insert_vps"] = n_vecs / results["insert_s"]

    # Enable INT8
    t0 = time.perf_counter()
    _ffi._lib.turbodb_vector_enable_int8(col._handle, 42)
    results["quantize_s"] = time.perf_counter() - t0

    results["mem_mb"] = col.memory_bytes() / 1024 / 1024

    # FP32 search (baseline for recall)
    fp32_results = []
    for q in queries:
        fp32_results.append(col.search(q, k=k, metric="cosine"))

    # INT8 parallel search
    def search_parallel(q):
        arr = (ctypes.c_float * len(q))(*q)
        indices = (ctypes.c_uint32 * k)()
        scores = (ctypes.c_float * k)()
        n = _ffi._lib.turbodb_vector_search_int8_parallel(col._handle, arr, len(q), k, 0, indices, scores)
        return [{"index": int(indices[i]), "score": float(scores[i])} for i in range(max(0, n))]

    t0 = time.perf_counter()
    par_results = [search_parallel(q) for q in queries]
    results["search_ms"] = (time.perf_counter() - t0) / len(queries) * 1000

    # Recall
    recall_sum = 0
    for fp32_r, par_r in zip(fp32_results, par_results):
        fp32_ids = set(r["index"] for r in fp32_r)
        par_ids = set(r["index"] for r in par_r)
        recall_sum += len(fp32_ids & par_ids) / max(len(fp32_ids), 1)
    results["recall"] = recall_sum / len(queries)

    col.close()
    return results


# ─── Zvec (in turbobox sandbox) ──────────────────────────────────────────

TURBOBOX_URL = os.environ.get("TURBOBOX_URL", "http://65.109.61.142:8080")

def sandbox_exec(box_id, cmd, timeout=300):
    """Execute command in turbobox sandbox."""
    import urllib.request
    url = f"{TURBOBOX_URL}/v1/boxes/{box_id}/exec"
    req = urllib.request.Request(url, data=cmd.encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
            return data.get("output", "")
    except Exception as e:
        return f"ERROR: {e}"


def sandbox_create():
    """Create a Python sandbox."""
    import urllib.request
    url = f"{TURBOBOX_URL}/v1/boxes"
    data = json.dumps({"image": "python", "cpu": 4000, "memory": 4096}).encode()
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["id"]


def sandbox_destroy(box_id):
    """Destroy sandbox."""
    import urllib.request
    url = f"{TURBOBOX_URL}/v1/boxes/{box_id}"
    req = urllib.request.Request(url, method="DELETE")
    try:
        urllib.request.urlopen(req)
    except:
        pass


def bench_zvec(dims, n_vecs, n_queries=10, k=10):
    """Run zvec benchmark in a turbobox sandbox."""
    results = {"engine": "Zvec", "dims": dims, "n_vecs": n_vecs}

    print(f"    Creating sandbox...", flush=True)
    box_id = sandbox_create()
    print(f"    Sandbox: {box_id}", flush=True)

    try:
        # Install zvec
        print(f"    Installing zvec...", flush=True)
        out = sandbox_exec(box_id, "uv pip install zvec numpy --quiet 2>&1 | tail -3", timeout=120)
        print(f"    {out.strip()}", flush=True)

        # Generate and run benchmark script
        script = f'''
import zvec, time, random, math, json, os

random.seed(42)
dims = {dims}
n = {n_vecs}
k = {k}
n_queries = {n_queries}

# Generate vectors
vecs = []
for _ in range(n):
    v = [random.gauss(0,1) for _ in range(dims)]
    norm = math.sqrt(sum(x*x for x in v))
    vecs.append([x/norm for x in v])

queries = []
for _ in range(n_queries):
    v = [random.gauss(0,1) for _ in range(dims)]
    norm = math.sqrt(sum(x*x for x in v))
    queries.append([x/norm for x in v])

# Create collection
schema = zvec.CollectionSchema(
    name="bench",
    vectors=zvec.VectorSchema("emb", zvec.DataType.VECTOR_FP32, dims),
)

path = "/tmp/zvec_bench"
os.makedirs(path, exist_ok=True)
col = zvec.create_and_open(path=path, schema=schema)

# Insert
t0 = time.perf_counter()
batch = []
for i, v in enumerate(vecs):
    batch.append(zvec.Doc(id=str(i), vectors={{"emb": v}}))
    if len(batch) >= 1000:
        col.insert(batch)
        batch = []
if batch:
    col.insert(batch)
insert_s = time.perf_counter() - t0

# Search
t0 = time.perf_counter()
for q in queries:
    r = col.query(zvec.VectorQuery("emb", vector=q), topk=k)
search_ms = (time.perf_counter() - t0) / n_queries * 1000

result = {{
    "insert_s": insert_s,
    "insert_vps": n / insert_s,
    "search_ms": search_ms,
    "n_vecs": n,
    "dims": dims,
}}
print("RESULT:" + json.dumps(result))
'''
        # Write script to sandbox
        escaped = script.replace("'", "'\\''")
        print(f"    Running benchmark (N={n_vecs:,}, d={dims})...", flush=True)
        out = sandbox_exec(box_id,
            f"python3 -c '{escaped}'",
            timeout=600)

        # Parse result
        for line in out.split("\n"):
            if line.startswith("RESULT:"):
                data = json.loads(line[7:])
                results.update(data)
                break
        else:
            results["error"] = out[:500]

    finally:
        sandbox_destroy(box_id)

    return results


# ─── Helpers ─────────────────────────────────────────────────────────────

def generate_vecs(n, dims):
    """Generate n unit-norm random vectors."""
    random.seed(42)
    for _ in range(n):
        v = [random.gauss(0, 1) for _ in range(dims)]
        norm = math.sqrt(sum(x * x for x in v))
        yield [x / norm for x in v]


def generate_queries(n, dims):
    random.seed(99)
    queries = []
    for _ in range(n):
        v = [random.gauss(0, 1) for _ in range(dims)]
        norm = math.sqrt(sum(x * x for x in v))
        queries.append([x / norm for x in v])
    return queries


# ─── Main ────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="TurboDB vs Zvec benchmark")
    ap.add_argument("--dims", type=int, nargs="+", default=[768, 1536])
    ap.add_argument("--sizes", type=int, nargs="+", default=[10000, 50000])
    ap.add_argument("--queries", type=int, default=10)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--skip-zvec", action="store_true", help="Only run TurboDB")
    ap.add_argument("--skip-turbodb", action="store_true", help="Only run Zvec")
    args = ap.parse_args()

    all_results = []

    print("=" * 90)
    print("  TurboDB vs Zvec Benchmark")
    print("=" * 90)

    for dims in args.dims:
        queries = generate_queries(args.queries, dims)
        for n in args.sizes:
            print(f"\n--- dims={dims}, N={n:,} ---")

            # TurboDB
            if not args.skip_turbodb:
                print(f"  TurboDB (INT8 parallel):", flush=True)
                try:
                    r = bench_turbodb(dims, n, queries, k=args.k)
                    print(f"    Insert: {r['insert_vps']:,.0f} vec/s ({r['insert_s']:.1f}s)")
                    print(f"    Search: {r['search_ms']:.1f} ms/query")
                    print(f"    Recall: {r['recall']:.3f}")
                    print(f"    Memory: {r['mem_mb']:.1f} MB")
                    all_results.append(r)
                except Exception as e:
                    print(f"    ERROR: {e}")

            # Zvec
            if not args.skip_zvec:
                print(f"  Zvec (in sandbox):", flush=True)
                try:
                    r = bench_zvec(dims, n, n_queries=args.queries, k=args.k)
                    if "error" in r:
                        print(f"    ERROR: {r['error'][:200]}")
                    else:
                        print(f"    Insert: {r.get('insert_vps', 0):,.0f} vec/s ({r.get('insert_s', 0):.1f}s)")
                        print(f"    Search: {r.get('search_ms', 0):.1f} ms/query")
                    all_results.append(r)
                except Exception as e:
                    print(f"    ERROR: {e}")

    # Summary table
    print(f"\n{'=' * 90}")
    print(f"  SUMMARY")
    print(f"{'=' * 90}")
    print(f"  {'Engine':<12} {'Dims':>6} {'N':>8} {'Insert/s':>10} {'Search ms':>10} {'Recall':>8} {'Mem MB':>8}")
    print(f"  {'-'*12} {'-'*6} {'-'*8} {'-'*10} {'-'*10} {'-'*8} {'-'*8}")
    for r in all_results:
        print(f"  {r.get('engine','?'):<12} {r.get('dims','?'):>6} {r.get('n_vecs','?'):>8,} "
              f"{r.get('insert_vps',0):>10,.0f} {r.get('search_ms',0):>10.1f} "
              f"{r.get('recall', '-'):>8} {r.get('mem_mb', '-'):>8}")

    # Save JSON
    out_path = os.path.join(os.path.dirname(__file__), "results.json")
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
