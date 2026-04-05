#!/usr/bin/env python3
"""
TurboDB Vector Benchmark
========================

Benchmarks FP32 brute-force vs TurboQuant quantized search across different
dimensions, dataset sizes, and bit widths.

Measures:
  - Insert throughput (vectors/sec)
  - Search latency (ms/query)
  - Memory usage (bytes)
  - Recall@10 (fraction of true top-10 found by quantized search)

Usage:
    zig build lib && python bench/vector_bench.py
"""

import json
import os
import sys
import time
import random

# Add project root to path for turbodb import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from turbodb import VectorColumn


def generate_vectors(n, dims, seed=42):
    """Generate n random unit-ish vectors of given dimensionality."""
    rng = random.Random(seed)
    vecs = []
    for _ in range(n):
        v = [rng.gauss(0, 1) for _ in range(dims)]
        norm = sum(x * x for x in v) ** 0.5
        if norm > 0:
            v = [x / norm for x in v]
        vecs.append(v)
    return vecs


def bench_insert(dims, n):
    """Benchmark insert throughput."""
    vecs = generate_vectors(n, dims)
    col = VectorColumn(dims=dims)

    t0 = time.perf_counter()
    for v in vecs:
        col.append(v)
    elapsed = time.perf_counter() - t0

    throughput = n / elapsed
    mem = col.memory_bytes()
    col.close()
    return {"throughput_vps": throughput, "elapsed_s": elapsed, "memory_bytes": mem}


def bench_insert_quantized(dims, n, bit_width):
    """Benchmark insert throughput with quantization enabled."""
    vecs = generate_vectors(n, dims)
    col = VectorColumn(dims=dims)

    # Insert first, then enable quantization (bulk quantize)
    for v in vecs:
        col.append(v)

    t0 = time.perf_counter()
    col.enable_quantization(bit_width=bit_width, seed=42)
    quant_elapsed = time.perf_counter() - t0

    mem = col.memory_bytes()
    col.close()
    return {
        "quant_elapsed_s": quant_elapsed,
        "memory_bytes": mem,
        "throughput_vps": n / quant_elapsed,
    }


def bench_search(dims, n, num_queries=50, k=10, metric="cosine"):
    """Benchmark FP32 brute-force search latency."""
    vecs = generate_vectors(n, dims)
    queries = generate_vectors(num_queries, dims, seed=999)

    col = VectorColumn(dims=dims)
    for v in vecs:
        col.append(v)

    # Warm up
    col.search(queries[0], k=k, metric=metric)

    t0 = time.perf_counter()
    results = []
    for q in queries:
        r = col.search(q, k=k, metric=metric)
        results.append(r)
    elapsed = time.perf_counter() - t0

    latency_ms = (elapsed / num_queries) * 1000
    col.close()
    return {"latency_ms": latency_ms, "total_s": elapsed, "results": results}


def bench_search_quantized(dims, n, bit_width, num_queries=50, k=10, metric="cosine"):
    """Benchmark quantized search latency and recall."""
    vecs = generate_vectors(n, dims)
    queries = generate_vectors(num_queries, dims, seed=999)

    col = VectorColumn(dims=dims)
    for v in vecs:
        col.append(v)

    # Get ground truth with FP32
    ground_truth = []
    for q in queries:
        r = col.search(q, k=k, metric=metric)
        ground_truth.append(set(item["index"] for item in r))

    # Enable quantization
    col.enable_quantization(bit_width=bit_width, seed=42)

    # Warm up
    col.search_quantized(queries[0], k=k, metric=metric)

    t0 = time.perf_counter()
    quant_results = []
    for q in queries:
        r = col.search_quantized(q, k=k, metric=metric)
        quant_results.append(r)
    elapsed = time.perf_counter() - t0

    # Compute recall@k
    recalls = []
    for gt, qr in zip(ground_truth, quant_results):
        found = set(item["index"] for item in qr)
        if len(gt) > 0:
            recalls.append(len(gt & found) / len(gt))
        else:
            recalls.append(1.0)
    avg_recall = sum(recalls) / len(recalls) if recalls else 0.0

    latency_ms = (elapsed / num_queries) * 1000
    mem = col.memory_bytes()
    col.close()
    return {
        "latency_ms": latency_ms,
        "total_s": elapsed,
        "recall_at_k": avg_recall,
        "memory_bytes": mem,
    }


def format_bytes(b):
    """Format bytes as human-readable string."""
    if b < 1024:
        return f"{b} B"
    elif b < 1024 * 1024:
        return f"{b / 1024:.1f} KB"
    else:
        return f"{b / (1024 * 1024):.1f} MB"


def run_benchmarks():
    """Run full benchmark suite."""
    dims_list = [128, 256, 768]
    n_list = [1_000, 10_000, 50_000, 100_000]
    bit_widths = [2, 4]
    k = 10
    num_queries = 50

    all_results = []

    print("=" * 100)
    print("TurboDB Vector Benchmark")
    print("=" * 100)
    print()

    for dims in dims_list:
        for n in n_list:
            print(f"--- dims={dims}, N={n:,} ---")

            # FP32 insert
            ins = bench_insert(dims, n)
            fp32_mem = ins["memory_bytes"]
            print(f"  FP32 insert:  {ins['throughput_vps']:,.0f} vec/s  |  mem: {format_bytes(fp32_mem)}")

            # FP32 search
            srch = bench_search(dims, n, num_queries=num_queries, k=k)
            fp32_latency = srch["latency_ms"]
            print(f"  FP32 search:  {fp32_latency:.3f} ms/query")

            result_row = {
                "dims": dims,
                "n": n,
                "fp32_insert_vps": ins["throughput_vps"],
                "fp32_memory_bytes": fp32_mem,
                "fp32_search_latency_ms": fp32_latency,
            }

            for bw in bit_widths:
                # Quantized insert (bulk quantize)
                qi = bench_insert_quantized(dims, n, bw)
                print(f"  Q{bw} quantize: {qi['throughput_vps']:,.0f} vec/s  |  mem: {format_bytes(qi['memory_bytes'])}")

                # Quantized search
                qs = bench_search_quantized(dims, n, bw, num_queries=num_queries, k=k)
                print(
                    f"  Q{bw} search:   {qs['latency_ms']:.3f} ms/query  |  "
                    f"recall@{k}: {qs['recall_at_k']:.3f}  |  "
                    f"mem: {format_bytes(qs['memory_bytes'])}"
                )

                result_row[f"q{bw}_quant_vps"] = qi["throughput_vps"]
                result_row[f"q{bw}_memory_bytes"] = qi["memory_bytes"]
                result_row[f"q{bw}_search_latency_ms"] = qs["latency_ms"]
                result_row[f"q{bw}_recall_at_{k}"] = qs["recall_at_k"]
                result_row[f"q{bw}_total_memory_bytes"] = qs["memory_bytes"]

            all_results.append(result_row)
            print()

    # Print comparison table
    print()
    print("=" * 120)
    print(f"{'dims':>5} {'N':>8} | {'FP32 ms':>9} {'FP32 mem':>10} | "
          f"{'Q2 ms':>8} {'Q2 recall':>9} {'Q2 mem':>10} | "
          f"{'Q4 ms':>8} {'Q4 recall':>9} {'Q4 mem':>10}")
    print("-" * 120)
    for r in all_results:
        print(
            f"{r['dims']:>5} {r['n']:>8,} | "
            f"{r['fp32_search_latency_ms']:>9.3f} {format_bytes(r['fp32_memory_bytes']):>10} | "
            f"{r.get('q2_search_latency_ms', 0):>8.3f} {r.get('q2_recall_at_10', 0):>9.3f} "
            f"{format_bytes(r.get('q2_total_memory_bytes', 0)):>10} | "
            f"{r.get('q4_search_latency_ms', 0):>8.3f} {r.get('q4_recall_at_10', 0):>9.3f} "
            f"{format_bytes(r.get('q4_total_memory_bytes', 0)):>10}"
        )
    print("=" * 120)

    # Save results to JSON
    out_path = os.path.join(os.path.dirname(__file__), "vector_bench_results.json")
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved to {out_path}")

    return all_results


if __name__ == "__main__":
    # Allow running a quick smoke test with smaller sizes
    if "--quick" in sys.argv:
        # Override for quick test
        import types

        orig = run_benchmarks

        def quick_bench():
            dims_list = [128]
            n_list = [1_000, 5_000]
            bit_widths = [2, 4]
            k = 10
            num_queries = 10

            all_results = []
            print("=" * 80)
            print("TurboDB Vector Benchmark (QUICK MODE)")
            print("=" * 80)
            print()

            for dims in dims_list:
                for n in n_list:
                    print(f"--- dims={dims}, N={n:,} ---")
                    ins = bench_insert(dims, n)
                    print(f"  FP32 insert:  {ins['throughput_vps']:,.0f} vec/s  |  mem: {format_bytes(ins['memory_bytes'])}")

                    srch = bench_search(dims, n, num_queries=num_queries, k=k)
                    print(f"  FP32 search:  {srch['latency_ms']:.3f} ms/query")

                    for bw in bit_widths:
                        qs = bench_search_quantized(dims, n, bw, num_queries=num_queries, k=k)
                        print(f"  Q{bw} search:   {qs['latency_ms']:.3f} ms/query  |  recall@{k}: {qs['recall_at_k']:.3f}")

                    print()
            return all_results

        quick_bench()
    else:
        run_benchmarks()
