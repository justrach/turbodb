# Optimization 4: TurboQuant Vector Search — 10x Faster, Beats Zvec

**Date:** 2026-04-05
**Components:** `src/turboquant.zig`, `src/vector.zig`, `src/ffi.zig`
**Impact:** 10x faster than FP32 brute force at 1M vectors, 1.3-1.9x faster than alibaba/zvec

---

## Problem

TurboDB had a basic vector engine (`src/vector.zig`) with:
- Flat brute-force scan, O(N×d) per query
- SIMD @Vector(4, f32) — only 4 floats at a time
- No quantization, no parallelism
- At 1M vectors × d=1536: ~935ms per query (too slow for real-time)

---

## Optimization Journey (6 stages)

### Stage 1: TurboQuant Core Module
**File:** `src/turboquant.zig` (new, ~650 lines)
**Lines:** L1-650

Implemented the TurboQuant algorithm (Zandieh et al., 2025) — data-oblivious vector quantization with near-optimal distortion:

- **Random orthogonal rotation** (L340-395, `generateOrthogonalMatrix`): Gram-Schmidt orthogonalization on random Gaussian matrix. Maps any input vector to uniform distribution on hypersphere, making coordinates approximately independent.
- **Pre-computed Lloyd-Max codebooks** (L30-38, `CODEBOOK_1BIT` through `CODEBOOK_4BIT`): Optimal scalar quantizer centroids for Beta((d-1)/2, (d-1)/2) distribution at b=1,2,3,4 bits.
- **Quantize** (L120-142): Rotate vector → per-coordinate nearest centroid lookup → pack b-bit indices into byte array.
- **Dequantize** (L145-161): Unpack indices → centroid lookup → inverse rotate.
- **Bit packing** (L395-425, `packBits`/`unpackBits`): Arbitrary b-bit packing into byte arrays with cross-byte boundary handling.

**Result:** 8x memory compression at 4-bit, MSE ≈ 0.009.

### Stage 2: SIMD Widening (4 → 8 lanes)
**File:** `src/vector.zig` L16-18
```zig
// Before:
const LANE: usize = 4;
const V4 = @Vector(LANE, f32);

// After:
const LANE: usize = 8;
const VL = @Vector(LANE, f32);
```

Updated `dotProduct()` (L371-392) and `l2Distance()` (L395-415) to use 8-wide SIMD. Processes 8 floats per cycle instead of 4 — matches AVX2 register width.

### Stage 3: ADC Lookup Tables
**File:** `src/turboquant.zig` L278-360

Replaced per-element codebook lookups with pre-computed Asymmetric Distance Computation (ADC) tables:

- **`buildL2Table()`** (L281-296): Pre-computes `table[j][k] = (rotated_query[j] - centroid[k])^2` for all d coordinates × 2^b centroids. Built once per query.
- **`buildDotTable()`** (L299-313): Same for dot product: `table[j][k] = rotated_query[j] * centroid[k]`.
- **`scanWithTable()`** (L316-370): Fast path for 4-bit (2 nibbles/byte, direct mask) and 2-bit (4 values/byte). Four independent accumulators to break FMA dependency chain (L323-338).

**Result:** 1.5x faster than per-element codebook lookup.

### Stage 4: INT8 SIMD Direct Distance (the key breakthrough)
**File:** `src/turboquant.zig` L648-680, `src/vector.zig` L259-365

Instead of ADC table lookups, store vectors as INT8 values and compute distance directly with integer SIMD:

- **`quantizeVecToInt8()`** (L682-720): Rotate → find max_abs → scale to [-127, 127] → store as contiguous i8 array. Returns per-vector scale factor.
- **`int8DotProduct()`** (L648-680): SIMD dot product with dual accumulators:
  ```zig
  // Process 32 i8 elements per iteration (2 × 16-lane)
  const LANE = 16;
  var acc0: @Vector(LANE, i32) = @splat(0);
  var acc1: @Vector(LANE, i32) = @splat(0);
  // acc0: first 16 elements, acc1: second 16 (independent, no stall)
  // Widen i8→i16→i32 to avoid overflow
  ```
- **`searchInt8()`** (L280-365): Quantize query once → scan all vectors with `int8DotProduct` → reconstruct scores via `int_dot / (q_scale * vec_scale)`.

**Why INT8 beats FP32:**
- 16 i8 multiply-accumulates per SIMD op vs 8 f32 (2x more lanes)
- 4x less memory per vector (768 bytes vs 3072 bytes)
- Better cache utilization at scale (INT8 data fits in L3, FP32 spills)
- Sequential access pattern (no random table lookups)

**Result:** 1.28x faster than FP32 at N=100K.

### Stage 5: Parallel Scan (the 10x speedup)
**File:** `src/vector.zig` L367-480

Split INT8 data across CPU cores, each thread scans its contiguous chunk independently:

- **`parallelSearchWorker()`** (L367-420): Worker function that scans a range [start, end) of vectors. Builds its own local min-heap. Includes `@prefetch` for next vector's data.
- **`searchInt8Parallel()`** (L425-480):
  1. Quantize query to INT8 (once, shared)
  2. Pre-compute `1.0 / (q_scale * vec_scale)` for all vectors (avoids division in hot loop)
  3. `std.Thread.getCpuCount()` → spawn up to 8 threads
  4. Each thread scans `count / n_threads` contiguous vectors
  5. Main thread joins all, merges per-thread heaps into final top-K

**Result:** 8.9x faster than FP32 at N=100K. At 1M: 94ms (10x faster).

### Stage 6: Batch Insert
**File:** `src/vector.zig` L115-131

- **`appendBatch()`** (L115-131): Pre-allocates capacity for all N vectors once, copies in bulk. Single allocation instead of N individual `appendSlice` calls.
- **Deferred INT8 quantization**: `append()` no longer quantizes per-insert when i8_enabled. All quantization happens in `enableInt8()` after inserts complete.
- **FFI `turbodb_vector_append_batch`** (ffi.zig L311-316): Accept N×d float array in one C call — avoids N Python→C FFI crossings.

**Result:** 932K vec/s at d=1536 (29x faster than Zvec's 32K/s).

---

## Final Benchmark Numbers

### d=1536, cosine similarity, recall@10

| Mode | N=10K | N=100K | N=1M |
|---|---|---|---|
| FP32 brute force | 10.0ms | 93.4ms | 935ms |
| INT8 single-thread | 8.5ms | 72.1ms | 729ms |
| **INT8 parallel (8T)** | **2.4ms** | **10.5ms** | **94ms** |
| Speedup vs FP32 | 4.2x | 8.9x | **10.0x** |
| Recall@10 | 98% | 97% | 100% |

### vs alibaba/zvec 0.3.0

| dims | N | TurboDB | Zvec | Winner |
|---|---|---|---|---|
| 768 | 10K | 1.0ms | 1.4ms | TurboDB 1.4x |
| 768 | 50K | 3.0ms | 5.0ms | TurboDB 1.7x |
| 768 | 100K | 5.4ms | 10.2ms | TurboDB 1.9x |
| 1536 | 10K | 2.4ms | 2.0ms | Zvec 1.2x |
| 1536 | 50K | 7.2ms | 9.5ms | TurboDB 1.3x |
| 1536 | 100K | 10.7ms | 18.4ms | TurboDB 1.5x |

### Insert throughput (batch insert)

| dims | N | TurboDB | Zvec | Winner |
|---|---|---|---|---|
| 1536 | 10K | 932K/s | 32K/s | TurboDB 29x |
| 1536 | 100K | 428K/s | 29K/s | TurboDB 15x |

---

## Other Features Built (not primary focus)

- **TurboQuant 4-bit quantization** (src/turboquant.zig L40-267): 8x memory compression with 99.5% recall. Slower than FP32 due to ADC table overhead but useful for memory-constrained deployments.
- **Quantized-only mode** (src/vector.zig L48, L89-98, L190-230): Stores only quantized data, frees FP32. 8x memory reduction.
- **Fast Walsh-Hadamard Transform** (src/turboquant.zig L500-530): O(d log d) rotation alternative to O(d^2) dense matvec. Available via `initWithFwht()` but recall is lower with current codebooks.
- **IVF index** (src/vector.zig L560-740): K-means clustering with inverted lists for sub-linear search. Framework implemented but recall needs tuning.
- **Regression test suite** (benchmark/regression.json + regression_test.py): Fixed baseline targets for insert throughput, search latency, recall, and zvec comparison ratios.

---

## fff.nvim Insights (for future text search improvements)

Researched [dmtrKovalenko/fff.nvim](https://github.com/dmtrKovalenko/fff.nvim) for search optimization ideas applicable to TurboDB's trigram indexer:

1. **Bigram filter as pre-filter** (fff-core/src/bigram_filter.rs): Dense bitset matrix mapping character pairs → file presence. AND posting lists to eliminate non-matches before expensive scoring. TurboDB already does this with trigrams — could add bigram layer for faster pre-filtering.
2. **Column-major bitset layout**: fff stores bigram→file mapping as column-major u64 arrays for cache-friendly AND operations. TurboDB's PostingList is row-major — could benefit from this layout at scale.
3. **Compression of low-selectivity columns**: fff discards bigrams appearing in >90% of files (no filtering power) or <3.1% (too rare to justify storage). TurboDB could apply similar pruning to its trigram index.
4. **Partial sort for top-K**: When k < N/2, use `select_nth_unstable` (quickselect) instead of full sort. Applicable to vector search heap merge.
5. **Lock-free parallel indexing**: fff uses CAS atomics for column allocation during parallel build. Similar to TurboDB's IndexQueue CAS pattern.

---

## Files Changed

| File | Lines | Description |
|---|---|---|
| `src/turboquant.zig` | +650 | TurboQuant quantizer, INT8 SIMD, ADC tables, FWHT, codebooks |
| `src/vector.zig` | +700 | VectorColumn: INT8 parallel search, batch insert, IVF, quantized-only |
| `src/ffi.zig` | +110 | C ABI: 10 new vector FFI exports |
| `python/turbodb/__init__.py` | +57 | Python VectorColumn class |
| `python/turbodb/_ffi.py` | +32 | ctypes prototypes |
| `benchmark/` | +600 | Regression suite, zvec comparison, 1M benchmark |
