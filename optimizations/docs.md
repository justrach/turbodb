# TurboDB Internal Optimization Docs

## Search Indexer (trigram)

| # | Title | Speedup | Key Technique |
|---|---|---|---|
| [1](optimization1.md) | Trigram Dedup | 2.9x (45s→15.7s) | Two-pass indexFile: collect unique trigrams lock-free, update global index locked |
| [2](optimization2.md) | File-ID Indirection | 4.1x (45s→10.9s) | Replace string-keyed posting lists with u32 file_ids. 1 alloc per file instead of 500 |
| [3](optimization3.md) | Dense Posting Lists | 13x (45s→3.5s) | ArrayList replaces HashMap per trigram. 16x less memory, cache-friendly linear scan |

## Vector Search

| # | Title | Speedup | Key Technique |
|---|---|---|---|
| [4](optimization4_vector_search.md) | TurboQuant + INT8 Parallel | 10x vs FP32, 1.8x vs Zvec | INT8 SIMD dot product (16 lanes) + parallel scan across 8 cores |

Six stages inside opt 4:
1. TurboQuant core — random rotation + Lloyd-Max codebooks (8x memory compression)
2. SIMD 4→8 lanes — @Vector(8, f32) for dotProduct/l2Distance
3. ADC lookup tables — pre-computed distance tables, 4-bit nibble fast path
4. INT8 SIMD direct distance — bypass tables, i8×i8 multiply-accumulate
5. Parallel scan — split data across 8 threads, merge heaps
6. Batch insert — appendBatch FFI, numpy zero-copy pointers

**Benchmarks (d=1536):**
- N=100K: 10.5ms/query (INT8 parallel) vs 93ms (FP32) vs 18.6ms (Zvec)
- N=1M: 94ms/query, 100% recall
- Insert: 932K/s (batch) vs Zvec 32K/s (29x faster)

## Code Layer

| # | Title | What | Key Technique |
|---|---|---|---|
| [5](optimization5_code_layer.md) | Code Layer | Git-like branching for agents | CoW branches as HashMap views, zero-copy fork, MVCC fallthrough |
| [6](optimization6_line_diff_conflict.md) | Line Diff + Conflicts | Per-line diffs + merge conflict detection | key_epochs tracking, greedy line matching with 5-line lookahead |

**Branch API (6 calls):**
```
create_branch   → O(1) fork
branch_write    → isolated CoW write
branch_read     → branch-first, fallthrough to main
branch_diff     → line-level diff (computed in Zig)
branch_merge    → merge with per-key conflict detection
compare_branches → side-by-side multi-branch review
```

**Plus:**
- `discover_context` — single-call: matching files + callers + tests
- `branch_search` — trigram search scoped to branch view
- 6 HTTP endpoints (`/branch/:col/...`)
- CDC auto-triggers on merge via existing insert→emitChange pipeline

## Architecture

```
Python / HTTP
    ↓
FFI (ctypes → libturbodb.dylib) or HTTP (server.zig)
    ↓
Collection
    ├── PageFile (mmap, 64KB pages)
    ├── BTree (key-value index)
    ├── TrigramIndex (text search, 13x optimized)
    ├── VectorColumn (INT8 parallel, 10x faster)
    ├── BranchManager (CoW branches, conflict detection)
    ├── WAL (crash recovery)
    ├── MVCC (version chain, time travel)
    └── CDC (webhooks on change)
```

## Performance Summary

| Operation | Latency | Notes |
|---|---|---|
| Insert (plain) | 138K/s | No vectors |
| Insert (with embedding, numpy) | 91K/s | Zero-copy pointer passing |
| Insert (batch, vector-only) | 932K/s | appendBatch FFI |
| Text search (50K docs) | 3.5ms | Trigram index, 13x optimized |
| Vector search (100K, d=1536) | 10.5ms | INT8 parallel, 8 threads |
| Vector search (1M, d=1536) | 94ms | 100% recall |
| Hybrid search (10K docs) | 2.7ms | Text pre-filter + vector re-rank |
| Branch create | ~1µs | Zero-copy epoch fork |
| Branch read (hit) | ~50ns | HashMap lookup |
| Branch read (miss) | ~1µs | Fallthrough to main B-tree |
| Branch merge | O(B×logN) | B = branch writes |
| Branch diff (line-level) | O(lines) | Greedy matching in Zig |
| Context discovery | ~0.1ms | 4-phase: trigram + callers + tests + MVCC |
