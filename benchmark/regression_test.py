#!/usr/bin/env python3
"""
TurboDB Vector Search Regression Test
======================================
Runs benchmarks and checks against regression.json targets.
Exits 0 if all pass, 1 if any regression detected.

Usage:
  python3 benchmark/regression_test.py              # full suite
  python3 benchmark/regression_test.py --quick      # 10K only
  python3 benchmark/regression_test.py --no-zvec    # skip zvec comparison
"""
import sys, os, time, json, ctypes, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))
import numpy as np
from turbodb import VectorColumn, _ffi

# Setup FFI
for name, args, res in [
    ("turbodb_vector_enable_int8", [ctypes.c_void_p, ctypes.c_uint64], ctypes.c_int),
    ("turbodb_vector_search_int8_parallel", [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32, ctypes.c_uint32, ctypes.c_uint8, ctypes.POINTER(ctypes.c_uint32), ctypes.POINTER(ctypes.c_float)], ctypes.c_int),
    ("turbodb_vector_append_batch", [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32, ctypes.c_uint32], ctypes.c_int),
]:
    getattr(_ffi._lib, name).argtypes = args
    getattr(_ffi._lib, name).restype = res

# Load targets
with open(os.path.join(os.path.dirname(__file__), "regression.json")) as f:
    TARGETS = json.load(f)


def search_parallel(handle, query, k=10):
    arr = (ctypes.c_float * len(query))(*query)
    idx = (ctypes.c_uint32 * k)()
    sc = (ctypes.c_float * k)()
    n = _ffi._lib.turbodb_vector_search_int8_parallel(handle, arr, len(query), k, 0, idx, sc)
    return [(int(idx[i]), float(sc[i])) for i in range(max(0, n))]


def search_fp32(col, query, k=10):
    r = col.search(query, k=k, metric="cosine")
    return [(x["index"], x["score"]) for x in r]


def run_bench(dims, n, n_queries=10):
    """Run insert + search + recall benchmark, return metrics dict."""
    np.random.seed(42)
    vecs = np.random.randn(n, dims).astype(np.float32)
    vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
    queries = np.random.randn(n_queries, dims).astype(np.float32)
    queries /= np.linalg.norm(queries, axis=1, keepdims=True)

    col = VectorColumn(dims)
    flat = vecs.flatten()
    arr = (ctypes.c_float * len(flat))(*flat)

    # Insert
    t0 = time.perf_counter()
    batch_sz = 10000
    for start in range(0, n, batch_sz):
        bsz = min(batch_sz, n - start)
        offset = start * dims
        ptr = ctypes.cast(ctypes.addressof(arr) + offset * 4, ctypes.POINTER(ctypes.c_float))
        _ffi._lib.turbodb_vector_append_batch(col._handle, ptr, dims, bsz)
    insert_s = time.perf_counter() - t0
    insert_vps = n / insert_s

    # Enable INT8
    _ffi._lib.turbodb_vector_enable_int8(col._handle, 42)

    # FP32 ground truth (for recall)
    ql = queries.tolist()
    fp32_results = [search_fp32(col, q, 10) for q in ql]

    # INT8 parallel search
    t0 = time.perf_counter()
    int8_results = [search_parallel(col._handle, q, 10) for q in ql]
    search_ms = (time.perf_counter() - t0) / n_queries * 1000

    # Recall
    recall_sum = 0
    for fp32_r, i8_r in zip(fp32_results, int8_results):
        fp32_ids = set(idx for idx, _ in fp32_r)
        i8_ids = set(idx for idx, _ in i8_r)
        recall_sum += len(fp32_ids & i8_ids) / max(len(fp32_ids), 1)
    recall = recall_sum / n_queries

    col.close()
    del arr
    return {"insert_vps": insert_vps, "search_ms": search_ms, "recall": recall}


def check_targets(results, quick=False):
    """Check results against regression targets. Returns (passed, failed) counts."""
    passed = 0
    failed = 0

    # Insert targets
    for t in TARGETS["insert"]["targets"]:
        dims, n, min_vps = t["dims"], t["n"], t["min_vps"]
        key = (dims, n)
        if key not in results:
            continue
        actual = results[key]["insert_vps"]
        ok = actual >= min_vps
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] Insert d={dims} N={n:>7,}: {actual:>9,.0f}/s (min: {min_vps:,})")
        if ok: passed += 1
        else: failed += 1

    # Search targets
    for t in TARGETS["search_int8_parallel"]["targets"]:
        dims, n, max_ms = t["dims"], t["n"], t["max_ms"]
        key = (dims, n)
        if key not in results:
            continue
        actual = results[key]["search_ms"]
        ok = actual <= max_ms
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] Search d={dims} N={n:>7,}: {actual:>7.1f}ms (max: {max_ms}ms)")
        if ok: passed += 1
        else: failed += 1

    # Recall targets
    for t in TARGETS["recall"]["targets"]:
        dims, n, min_recall = t["dims"], t["n"], t["min_recall"]
        key = (dims, n)
        if key not in results:
            continue
        actual = results[key]["recall"]
        ok = actual >= min_recall
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] Recall d={dims} N={n:>7,}: {actual:.3f} (min: {min_recall})")
        if ok: passed += 1
        else: failed += 1

    return passed, failed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="Only run 10K tests")
    args = ap.parse_args()

    sizes = [10000] if args.quick else [10000, 50000, 100000]

    print("=" * 70)
    print("  TurboDB Vector Regression Test")
    print("=" * 70)

    results = {}
    for dims in [768, 1536]:
        for n in sizes:
            print(f"\n  Running d={dims} N={n:,}...", end=" ", flush=True)
            r = run_bench(dims, n)
            results[(dims, n)] = r
            print(f"insert={r['insert_vps']:,.0f}/s search={r['search_ms']:.1f}ms recall={r['recall']:.3f}")

    print(f"\n{'=' * 70}")
    print("  Regression Checks")
    print(f"{'=' * 70}")
    passed, failed = check_targets(results, quick=args.quick)

    print(f"\n{'=' * 70}")
    if failed == 0:
        print(f"  ALL {passed} CHECKS PASSED")
        print(f"{'=' * 70}")
        return 0
    else:
        print(f"  {failed} REGRESSION(S) DETECTED ({passed} passed)")
        print(f"{'=' * 70}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
