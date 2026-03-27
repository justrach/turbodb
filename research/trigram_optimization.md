# Trigram Index Optimization Research

**Context**: TurboDB currently bottlenecks at 87% of indexing time in nested `StringHashMap` lookups, achieving 3,275 files/s. Target: 10K+ files/s.

**Date**: 2026-03-27

---

## Table of Contents

1. [Google Code Search (Russ Cox 2012)](#1-google-code-search-russ-cox-2012)
2. [Sourcegraph Zoekt](#2-sourcegraph-zoekt)
3. [Cursor's Sparse N-Gram Index](#3-cursors-sparse-n-gram-index)
4. [ClickHouse Inverted Index](#4-clickhouse-inverted-index)
5. [Flat/Arena-Based Alternatives to Nested HashMaps](#5-flatarena-based-alternatives-to-nested-hashmaps)
6. [Compact Posting List Representations](#6-compact-posting-list-representations)
7. [SIMD Trigram Extraction](#7-simd-trigram-extraction)
8. [Lock-Free Concurrent Indexing](#8-lock-free-concurrent-indexing)
9. [Tantivy (Rust)](#9-tantivy-rust)
10. [Recommendations for TurboDB](#10-recommendations-for-turbodb)

---

## 1. Google Code Search (Russ Cox 2012)

**Source**: [Regular Expression Matching with a Trigram Index](https://swtch.com/~rsc/regexp/regexp4.html) | [github.com/google/codesearch](https://github.com/google/codesearch)

### Data Structure

- **Inverted index**: maps each trigram (3-byte substring) to a sorted posting list of document IDs
- **Index file**: contains path list, indexed file list, then one posting list per trigram, written sequentially
- **Query**: regex is decomposed into AND/OR expressions of required trigrams; posting lists are intersected/unioned to find candidate documents, then full regex runs only on candidates

### Performance

| Metric | Value |
|--------|-------|
| Index size | ~20% of corpus size (420MB Linux kernel -> 77MB index) |
| Search speedup | ~100x (36,972 files narrowed to 25 candidates for "hello world") |
| I/O strategy | `mmap` for index file; OS page cache eliminates server process |

### Key Optimization Insight

Trigrams are the sweet spot: bigrams (64K keys) produce posting lists that are too large; quadgrams (billions of keys) produce too many keys. The trigram query generation rules convert any regex into an AND/OR boolean query over trigrams, preserving correctness while drastically reducing candidate sets. The implementation rejects files with invalid UTF-8 or excessive distinct trigrams to keep the index clean.

---

## 2. Sourcegraph Zoekt

**Source**: [Zoekt Memory Optimizations](https://sourcegraph.com/blog/zoekt-memory-optimizations-for-sourcegraph-cloud) | [github.com/sourcegraph/zoekt](https://github.com/sourcegraph/zoekt)

### Data Structure Evolution

Zoekt went through 4 optimization stages to replace its original `map[ngram]simpleSection`:

| Stage | Structure | Memory (19K repos, 2.6B lines) |
|-------|-----------|-------------------------------|
| Original | Go `map[uint64]simpleSection` | 15 GB (40 bytes/entry overhead) |
| Stage 1 | Sorted `[]ngram` + `[]uint32` with binary search | 5 GB |
| Stage 2 | Two-level split: top 32-bit + bottom 32-bit arrays | 3.5 GB |
| Stage 3 | ASCII/Unicode split (7-bit ASCII fast path) | 2.3 GB |
| Combined | All optimizations + compressed filename postings | 4 GB total (from 22 GB) |

### Performance

| Metric | Value |
|--------|-------|
| RAM per repo | 1,400 KB -> 310 KB (5x reduction) |
| Search latency | No measurable change after optimization |
| Index size on disk | ~3x corpus size (2x offsets + 1x content) |
| Lookup complexity | O(log N) binary search (acceptable; not a bottleneck) |

### Key Optimization Insight

**The Go map was the #1 memory bottleneck** (67% of RAM). Replacing it with sorted arrays + binary search gave a 3x reduction. Splitting 64-bit ngram keys into two 32-bit halves (first character vs. last two) gave another 30% reduction. A three-level trie was tested but used 20% more memory than the two-level split due to pointer overhead. Additionally, shrinking dynamically-grown slices to exact capacity saved 500MB across the corpus.

**This is directly analogous to TurboDB's problem**: nested `StringHashMap` has high per-entry overhead. Sorted arrays with binary search are a proven production solution.

---

## 3. Cursor's Sparse N-Gram Index

**Source**: [Fast regex search: indexing text for agent tools](https://cursor.com/blog/fast-regex-search) (March 2025)

### Data Structure

Cursor's index uses **three progressive approaches**:

#### 3a. Phrase-Aware Trigram Index

- Standard trigram keys, but each posting stores **2 extra bytes per document**:
  - `locMask` (8 bits): bloom filter for position `pos mod 8` where trigram appears
  - `nextMask` (8 bits): bloom filter of hashed follow-up characters after the trigram
- This effectively creates "3.5-grams" without the key explosion of true quadgrams
- Eliminates false positives from position aliasing and adjacent-character ambiguity

#### 3b. Sparse N-Grams (ClickHouse-derived)

- Instead of extracting every consecutive 3-char sequence, assigns a **deterministic weight** to each character pair (CRC32 hash or character-pair frequency table)
- N-grams are all substrings where weights at both ends are strictly greater than all weights inside
- Variable-length n-grams (not fixed 3-char); more n-grams generated at index time but **far fewer lookups at query time**
- **build_all** (index time): extract all possible sparse n-grams from each document
- **build_covering** (query time): extract only minimal covering n-grams at string edges

Using character-pair frequency from real source code as the weight function produces even fewer covering n-grams, because rare character pairs get high weights and become natural segment boundaries.

#### 3c. On-Disk Layout

- **Two files**:
  1. **Postings file**: all posting lists flushed sequentially to disk during construction
  2. **Lookup table**: sorted table of n-gram hashes + offsets into postings file
- Only hashes stored (no full n-gram strings); hash collisions broaden posting lists but never produce incorrect results
- **Lookup table is `mmap`'d** in the editor process; queries use binary search on the table, then read directly at the returned offset in the postings file
- Minimal memory footprint: only the lookup table resides in memory

### Performance

| Metric | Value |
|--------|-------|
| Memory model | mmap'd lookup table only; postings read from disk on demand |
| Query overhead | Minimal: binary search on sorted hash table + single disk read |
| Index build cost | Higher than classic trigrams (more n-grams extracted) |
| Query selectivity | Much higher than trigrams (fewer false positive documents) |

### Key Optimization Insight

**Trade expensive indexing for cheap queries.** Sparse n-grams with frequency-weighted pair selection produce variable-length tokens that are far more selective than fixed trigrams. The on-disk two-file layout (mmap'd hash table + sequential postings) keeps editor memory minimal while still providing fast search. The covering algorithm at query time needs only edge n-grams, making queries significantly cheaper than the indexing step.

---

## 4. ClickHouse Inverted Index

**Source**: [Introducing Inverted Indices in ClickHouse](https://clickhouse.com/blog/clickhouse-search-with-inverted-indices) | [Text Indexes Docs](https://clickhouse.com/docs/engines/table-engines/mergetree-family/textindexes)

### Data Structure

- **Dictionary**: Finite State Transducers (FSTs) — shared prefixes between adjacent terms are removed, reducing memory footprint
- **Posting lists**: stored as **roaring bitmaps** (compressed sorted lists of 32-bit integers)
- **Segmented index**: multiple sub-indexes per part, each covering a consecutive row range; controlled by `max_digestion_size_per_segment`
- **Three files on disk**:
  1. **Metadata file**: array of segment descriptors (segment ID, start row, dictionary offset, posting list offset)
  2. **Posting list file**: all posting lists for all segments, concatenated
  3. **Dictionary file**: per-segment minimized FSTs + dictionary sizes

### Tokenization Options

- `tokenbf_v1`: splits on whitespace, stores tokens in bloom filter (probabilistic)
- `ngrambf_v1`: splits into fixed n-grams (usually 3 or 4), stores in bloom filter (probabilistic)
- `inverted`: full deterministic inverted index with roaring bitmap posting lists (no false positives)
- `sparseGrams`: variable-length n-grams (min/max length configurable) — the sparse n-gram algorithm

### Performance

| Metric | Value |
|--------|-------|
| Search (28.7M Hacker News comments) | 0.843s -> 0.248s with inverted index (3.4x speedup) |
| Granule filtering | 554/3528 granules selected for "clickhouse" query |
| Index construction | Single-pass in-memory inversion; processes configurable chunks |

### Key Optimization Insight

**Single-pass in-memory inversion** with configurable chunk size: iterates column data once, processes in bounded memory, writes index directly without intermediate files. The combination of FST dictionaries (compact) + roaring bitmap posting lists (fast intersection/union) is the production standard for columnar stores. The index is never fully loaded into memory — only pieces are read as needed.

---

## 5. Flat/Arena-Based Alternatives to Nested HashMaps

### The Problem

Nested hashmaps (e.g., `StringHashMap(ArrayList(DocId))`) suffer from:
- **40+ bytes overhead per entry** (Go maps; similar for most hash table implementations)
- **Pointer chasing**: each posting list is a separate heap allocation, destroying cache locality
- **Allocator pressure**: millions of small allocations fragment the heap

### Approach A: Direct-Mapped Array (for ASCII trigrams)

For 7-bit ASCII, there are only `128^3 = 2,097,152` possible trigrams (~2M). A flat array of 2M entries, each pointing to a posting list, eliminates all hash computation:

```
// Trigram key = byte[0] << 16 | byte[1] << 8 | byte[2]
// Direct index: O(1) lookup, zero hash overhead
posting_lists: [2_097_152]PostingListHeader
```

- **Memory**: 2M * 8 bytes (offset + length) = 16 MB fixed overhead — trivial
- **Lookup**: single array index, no hash, no collision handling
- **Limitation**: only works for 7-bit ASCII; Unicode trigrams need a fallback

This is the approach Zoekt effectively approximates with its ASCII/Unicode split.

### Approach B: Arena-Allocated Posting Lists

**Source**: [Arena-friendly hash map](https://nullprogram.com/blog/2023/09/30/) | [Arena allocator tips](https://nullprogram.com/blog/2023/09/27/)

Replace per-posting-list heap allocations with a single contiguous arena:

```
arena: []u8  // single large allocation, grows by doubling
posting_lists: [N]struct { offset: u32, len: u32 }  // index into arena

// Appending a doc_id to a posting list:
// 1. Write doc_id at arena[arena_pos]
// 2. Advance arena_pos
// 3. Increment posting_lists[trigram].len
```

Benefits:
- **O(1) allocation**: pointer bump, no free-list traversal
- **Cache-friendly**: sequential writes during indexing; sequential reads during search
- **Zero fragmentation**: single deallocation frees everything
- **6-8x faster** than system allocator for allocation-heavy workloads

### Approach C: Zoekt-Style Sorted Arrays

As proven in production (see Section 2):

- Store trigram keys in a sorted `[]u64` array
- Store posting list offsets in a parallel `[]u32` array
- Lookup via binary search: O(log N) but excellent cache behavior for sorted data
- **3-5x less memory** than equivalent hash map

### Key Optimization Insight

**For TurboDB specifically**: the 87% time in `StringHashMap` lookups can likely be eliminated by combining approaches A + B:
1. Direct-mapped array for ASCII trigrams (covers ~95% of source code)
2. Arena-allocated posting lists with contiguous memory layout
3. Fallback sorted array for Unicode trigrams

This eliminates hash computation, collision handling, and pointer chasing in the hot path.

---

## 6. Compact Posting List Representations

### 6a. Roaring Bitmaps

**Source**: [Elastic Blog: Frame of Reference and Roaring Bitmaps](https://www.elastic.co/blog/frame-of-reference-and-roaring-bitmaps) | [roaringbitmap.org](https://roaringbitmap.org/)

- Split 32-bit integer space into chunks of 2^16; upper 16 bits select a chunk, lower 16 bits stored in a container
- Three container types per chunk:
  - **Array container**: sorted `u16[]` for sparse chunks (< 4096 values)
  - **Bitmap container**: 8KB bitset for dense chunks (>= 4096 values)
  - **Run-length container**: for consecutive runs
- Intersection: optimized algorithms for every pair of container types
- Used in production by: ClickHouse, Lucene/Elasticsearch, Apache Druid, Pilosa

| Metric | Value |
|--------|-------|
| Intersection speed | Up to 900x faster than compressed alternatives |
| Compression | ~2x better than delta-encoded alternatives in many cases |
| Random access | O(1) via container lookup |

### 6b. Delta Encoding + Bit Packing (Lucene/Tantivy style)

- Posting lists are sorted; store deltas between consecutive doc IDs
- Pack deltas into blocks of 128 using minimal bits (bit packing)
- Tantivy interleaves doc IDs and term frequencies in blocks of 128

| Metric | Value |
|--------|-------|
| Decode speed (TurboPFor) | 3-10 billion integers/s with AVX2 |
| S4-BP128-D4 | 0.7 CPU cycles per decoded integer |
| Compression | Excellent for clustered doc IDs |

### 6c. PForDelta / TurboPFor

**Source**: [TurboPFor-Integer-Compression](https://github.com/powturbo/TurboPFor-Integer-Compression) | [SIMD Compression and Intersection of Sorted Integers](https://arxiv.org/abs/1401.6399)

- Compresses blocks of 128 d-gaps; chooses smallest bit-width where 90% of values fit
- Exceptions (values needing more bits) stored separately
- TurboPFor variant adds SIMD/AVX2 direct access: 4x faster than OptPFD
- SIMD-accelerated posting list intersection can double the speed of state-of-the-art

| Metric | Value |
|--------|-------|
| TurboPFor decode | 3+ billion integers/s (Skylake, 3.7GHz) |
| TurboPack AVX2 bitpacking | 10 billion integers/s decode |
| Compression ratio | Competitive with Roaring; better on uniform distributions |

### Key Optimization Insight

**For small posting lists** (which most trigram entries have — Zipf distribution), a simple delta-encoded `u32[]` in an arena is likely optimal. **For large posting lists** (common trigrams like `the`, `ing`), roaring bitmaps or PForDelta provide both compression and fast intersection. A hybrid approach — small lists inline, large lists compressed — is what production systems converge on.

---

## 7. SIMD Trigram Extraction

### The Opportunity

Trigram extraction from a byte stream is embarrassingly parallel: for each position `i`, emit `bytes[i..i+3]`. A 32-byte AVX2 register can process 30 trigrams per instruction.

### Approach A: Vectorized Byte Classification (simdjson technique)

**Source**: [simdjson](https://github.com/simdjson/simdjson) | [Parsing Gigabytes of JSON per Second](https://arxiv.org/html/1902.08318v7)

The simdjson two-nibble classification technique can identify byte classes in 16/32/64 bytes per instruction:
1. Split each byte into high nibble and low nibble
2. Use `vpshufb` (AVX2) to look up each nibble in a 16-entry table
3. AND results together — only correct class survives

Applied to trigram extraction:
- Classify bytes to detect non-indexable characters (null bytes, control chars) in bulk
- Process 30 trigrams per 32-byte SIMD load
- Use scatter/gather to hash and insert trigrams into posting lists

### Approach B: Rolling Hash with SIMD

For sparse n-grams with CRC32-based weights:
- `_mm256_crc32_epi8` or equivalent can compute CRC32 of character pairs in bulk
- Compare adjacent weights to find segment boundaries
- Extract n-grams at boundaries

### Approach C: Teddy Algorithm (ripgrep/Hyperscan)

**Source**: [ripgrep benchmarks](https://burntsushi.net/ripgrep/) | [Hyperscan paper](https://www.usenix.org/system/files/nsdi19-wang-xiang.pdf)

Ripgrep's Teddy algorithm uses packed SIMD comparisons of 16 bytes at a time to find candidate match locations for multiple patterns simultaneously. Hyperscan extends this with decomposition-based SIMD acceleration for both string and FA matching.

### Key Optimization Insight

**For TurboDB**: the trigram extraction loop itself is unlikely the bottleneck (it's the hashmap insertions). However, SIMD can help in two ways:
1. **Bulk trigram hashing**: compute trigram keys for 30 positions simultaneously using AVX2
2. **Sparse n-gram boundary detection**: vectorized weight comparison to find segment boundaries
3. **Posting list intersection**: SIMD-accelerated sorted list intersection during search

---

## 8. Lock-Free Concurrent Indexing

### Approach A: Thread-Local Segments (Lucene/Tantivy)

**Source**: [Lucene IndexWriter](https://www.alibabacloud.com/blog/lucene-indexwriter-an-in-depth-introduction_594673) | [Tantivy Architecture](https://github.com/quickwit-oss/tantivy/blob/main/ARCHITECTURE.md)

The production-proven approach:
1. Each thread gets its own `DocumentsWriterPerThread` (DWPT) — independent buffer, tokenizer, and posting lists
2. Zero lock contention during indexing: each thread writes to its own segment
3. Segments are flushed independently to disk
4. Background merge process combines small segments into larger ones (ConcurrentMergeScheduler)

| Metric | Value |
|--------|-------|
| Tantivy indexing | 12.5K docs/s single-node (vs. Elasticsearch 3.2K) |
| RAM | 75% lower than Elasticsearch (no JVM overhead) |
| Startup | < 10ms |

### Approach B: Atomic Posting List Updates (Bw-tree style)

- Copy-on-write delta records prepended to posting list pages
- Installed via atomic CAS on a mapping table
- Lock-free but adds complexity and indirection

### Approach C: Sharded Index with Lock-Free Queues

- Partition trigram space into N shards (e.g., by first byte of trigram)
- Each shard owned by one thread; files dispatched to shards via lock-free MPSC queue
- Eliminates contention entirely at the cost of cross-shard coordination for queries

### Key Optimization Insight

**Thread-local segments with background merge is the industry standard.** It's simpler than lock-free data structures, eliminates contention entirely during the indexing hot path, and the merge cost is amortized. For TurboDB at 3,275 files/s targeting 10K+, parallel indexing across N cores with thread-local posting lists could provide a near-linear speedup if the hashmap bottleneck is also addressed.

---

## 9. Tantivy (Rust)

**Source**: [Tantivy introduction](https://fulmicoton.com/posts/behold-tantivy/) | [Tantivy indexing](https://fulmicoton.com/posts/behold-tantivy-part2/) | [github.com/quickwit-oss/tantivy](https://github.com/quickwit-oss/tantivy)

### Data Structure

- **Segment-based architecture**: each segment is an immutable unit with its own inverted index
- **Term dictionary**: Finite State Transducers (FSTs) for compact term-to-offset mapping
- **Posting lists**: doc IDs and term frequencies interleaved in blocks of 128, bit-packed with minimal bit width per block
- **Positions**: stored separately, also compressed

### Performance

| Metric | Value |
|--------|-------|
| Indexing throughput | 12.5K docs/s (single node) |
| vs. Elasticsearch | ~4x faster indexing, 2x faster search |
| RAM usage | 75% less than Elasticsearch |
| Startup time | < 10ms |

### Key Optimization Insight

Rust's ownership model enables **zero-cost thread safety** — parallel indexing and queries without locks or runtime overhead. The block-based posting list compression (128 doc IDs per block, interleaved with term frequencies) provides excellent cache behavior. FSTs for term dictionaries share the same insight as ClickHouse: prefix compression of sorted terms.

---

## 10. Recommendations for TurboDB

Based on this research, here is a prioritized action plan to move from 3,275 files/s to 10K+ files/s:

### Priority 1: Replace Nested StringHashMap (Highest Impact)

**Current bottleneck: 87% of indexing time**

| Option | Complexity | Expected Speedup |
|--------|-----------|-----------------|
| Direct-mapped `[2M]PostingHeader` for ASCII trigrams | Low | 5-10x for lookups |
| Arena-allocated posting lists (pointer bump) | Low | 3-5x for allocation |
| Zoekt-style sorted arrays (for final index) | Medium | 3-5x memory reduction |

**Recommended approach**:
1. Replace `StringHashMap` with a flat `[128^3]u32` array mapping ASCII trigrams to arena offsets
2. Allocate posting list data from a single contiguous arena (bump allocator)
3. For Unicode trigrams (< 5% of keys), use a separate compact hash table or sorted array

### Priority 2: Parallel File Indexing (Linear Scaling)

| Option | Complexity | Expected Speedup |
|--------|-----------|-----------------|
| Thread-local index segments + merge | Medium | Near-linear with core count |
| Sharded trigram space | Medium | Near-linear, simpler queries |

**Recommended approach**: Follow Lucene/Tantivy's thread-local segment model:
1. Each thread maintains its own arena + direct-mapped trigram array
2. After batch completes, merge segments (posting list concatenation + sort)
3. N threads -> ~Nx throughput

### Priority 3: Compact Posting Lists (Memory + Search Speed)

| Option | Best For | Complexity |
|--------|----------|-----------|
| Delta-encoded u32[] in arena | Small lists (< 256 entries) | Low |
| Roaring bitmaps | Large lists, fast intersection | Medium |
| PForDelta blocks of 128 | Balanced compression + speed | High |

**Recommended approach**: Start with simple delta-encoded lists in the arena. Profile search workload before adding roaring bitmaps for large posting lists.

### Priority 4: Sparse N-Grams (Query Selectivity)

Consider adopting Cursor's sparse n-gram approach for the on-disk index format:
- Higher indexing cost but dramatically fewer false positives at query time
- Two-file layout (mmap'd hash table + sequential postings) keeps runtime memory minimal
- Character-pair frequency weighting from real source code improves selectivity further

### Priority 5: SIMD Optimization (Polish)

Only after the above are implemented:
- Vectorized trigram key computation (30 trigrams per AVX2 load)
- SIMD-accelerated posting list intersection for search
- Vectorized weight computation for sparse n-gram boundary detection

### Expected Combined Impact

| Optimization | Estimated Speedup |
|-------------|------------------|
| Flat array + arena (replace hashmap) | 3-5x |
| Parallel indexing (4 cores) | 3-4x |
| Combined | 10-20x (target: 30K-65K files/s) |

The hashmap replacement alone should bring TurboDB close to the 10K files/s target. Adding parallelism should push well beyond it.

---

## References

1. Russ Cox, "Regular Expression Matching with a Trigram Index" (2012) — https://swtch.com/~rsc/regexp/regexp4.html
2. Sourcegraph, "A 5x reduction in RAM usage with Zoekt memory optimizations" (2021) — https://sourcegraph.com/blog/zoekt-memory-optimizations-for-sourcegraph-cloud
3. Cursor, "Fast regex search: indexing text for agent tools" (2025) — https://cursor.com/blog/fast-regex-search
4. ClickHouse, "Introducing Inverted Indices in ClickHouse" (2023) — https://clickhouse.com/blog/clickhouse-search-with-inverted-indices
5. ClickHouse, "Full-text Search with Text Indexes" — https://clickhouse.com/docs/engines/table-engines/mergetree-family/textindexes
6. TurboPFor Integer Compression — https://github.com/powturbo/TurboPFor-Integer-Compression
7. Lemire et al., "SIMD Compression and the Intersection of Sorted Integers" (2014) — https://arxiv.org/abs/1401.6399
8. Elastic, "Frame of Reference and Roaring Bitmaps" — https://www.elastic.co/blog/frame-of-reference-and-roaring-bitmaps
9. Tantivy — https://github.com/quickwit-oss/tantivy
10. Chris Wellons, "An easy-to-implement, arena-friendly hash map" (2023) — https://nullprogram.com/blog/2023/09/30/
11. simdjson — https://github.com/simdjson/simdjson
12. Hyperscan — https://www.usenix.org/system/files/nsdi19-wang-xiang.pdf
13. ripgrep — https://burntsushi.net/ripgrep/
14. Lucene IndexWriter Architecture — https://www.alibabacloud.com/blog/lucene-indexwriter-an-in-depth-introduction_594673
15. Zoekt Design Doc — https://github.com/sourcegraph/zoekt/blob/main/doc/design.md
