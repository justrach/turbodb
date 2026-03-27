# Posting List Compression Research

Target scale: 30K-100K unique trigrams, 60K-200M files (file IDs are u32).

---

## 1. Roaring Bitmaps

### How They Work

Roaring bitmaps are a two-level compressed bitmap data structure for sets of 32-bit integers. Each integer is split into its upper 16 bits (chunk key) and lower 16 bits (value within chunk).

**Top level:** sorted array of 16-bit chunk keys + pointers to containers.

**Container types** (chosen dynamically per chunk):

| Container | When Used | Storage | Size |
|-----------|-----------|---------|------|
| **Array** | <= 4,096 values in chunk | Sorted packed array of 16-bit values | 2 bytes/value (max 8 KB) |
| **Bitmap** | > 4,096 values in chunk | Fixed 2^16 bit array | Always 8 KB |
| **Run** | Consecutive sequences | (start, length) pairs, 16-bit each | 4 bytes/run |

The 4,096 threshold is the crossover point where an array container (4096 * 2 = 8192 bytes) equals a bitmap container (2^16 / 8 = 8192 bytes).

### Operations

- **Insertion:** Find chunk by upper 16 bits, insert into container. Container type may change (array -> bitmap at 4096 elements).
- **Membership:** Binary search on array containers, direct bit check on bitmap containers.
- **Intersection:** Algorithm varies by container pair:
  - Bitmap/Bitmap: bitwise AND
  - Bitmap/Array: iterate array, check each in bitmap
  - Array/Array: merge or galloping intersection (based on cardinality ratio)
- **Union:** Similar dispatch; Bitmap/Bitmap is bitwise OR.

### Space Overhead vs Raw Sorted u32

- Sparse sets (< 4096 per chunk): 2 bytes/element vs 4 bytes/element = **50% of raw**
- Dense sets: 1 bit/element via bitmap = **orders of magnitude smaller**
- Very sparse (< 0.05% density): slightly worse than plain arrays due to per-chunk overhead
- Worst case (uniformly random across many chunks): ~16 bits/integer = same as raw u16 per value

### Intersection Speed

- Bitmap/bitmap intersection uses bitwise AND, extremely fast with SIMD
- Array/array uses galloping intersection for skewed cardinalities
- CRoaring uses AVX2/AVX-512/NEON SIMD for array intersection, union, difference, symmetric difference
- Generally the fastest compressed format for intersection across all densities (Elastic blog benchmarks show roaring is never a bad choice)

### Build Speed

- Array containers: sorted insert is O(n) per element (shift right)
- Sequential construction with RoaringBitmapWriter: optimal, avoids container conversions
- `runOptimize()` can be called post-build to find run containers

### Sweet Spots

- **Wins at:** mixed-density workloads, in-memory filter caches, sets that need fast boolean operations
- **Loses at:** very sparse sets with very few elements per chunk (plain sorted arrays win), on-disk storage where decode speed matters less than compression ratio
- At our scale: with 200M file IDs, chunks will have ~3,052 values on average if uniform (200M / 65536). Many trigrams will have far fewer docs (sparse -> array containers) while common trigrams will be dense (bitmap containers). Roaring adapts automatically.

### Implementations

- **CRoaring** (C/C++): reference implementation, SIMD-optimized (AVX2, AVX-512, NEON). Used by ClickHouse, Apache Doris, Redpanda, StarRocks.
- **roaring-zig**: Zig bindings for CRoaring (github.com/jwhear/roaring-zig)
- **zroaring**: Pure Zig implementation (recent, posted on ziggit.dev March 2026)
- **roaring-rs**: Rust implementation, pure Rust

---

## 2. Delta Encoding + Varint

### How It Works

Given a sorted list of doc IDs, replace absolute values with gaps (deltas) between consecutive values:

```
[7, 12, 15, 17, 25] -> [7, 5, 3, 2, 8]
```

Deltas are smaller than absolute values, so they can be encoded with fewer bits. Variable-byte (varint) encoding then stores each delta using 1-5 bytes, where the MSB of each byte signals continuation.

### Variants

| Variant | Description | Decode Speed |
|---------|-------------|-------------|
| **VByte** | Classic 1-byte-at-a-time, MSB continuation | Slow (branch per byte) |
| **Group Varint (Varint-GB)** | Groups 4 integers, 1 control byte for lengths | ~2x faster than VByte |
| **Varint-G8IU** | 8 data bytes + 1 control byte, variable count | Good SIMD potential |
| **Stream-VByte** | Separates control and data streams | ~2x faster than G8IU, best SIMD |
| **Masked-VByte** | SIMD-accelerated VByte decode | Good throughput |

### Space Overhead vs Raw u32

- Depends entirely on gap distribution
- For average gap of 100: ~7-8 bits per integer (vs 32 bits raw) = **~22% of raw**
- For average gap of 10: ~4-5 bits per integer = **~14% of raw**
- For average gap of 1000: ~10-11 bits per integer = **~31% of raw**
- VByte plateaus at 8 bits/integer minimum (1 byte per value) for moderate gaps

### Intersection Speed

- Requires sequential decode -- cannot skip efficiently without skip pointers
- Skip lists layered on top restore O(log n) skip capability
- SIMD variants decode billions of integers/second but intersection still needs decode-then-compare

### Build Speed

- Extremely fast: single pass, O(n), no complex data structures
- Just compute deltas and encode each one

### Sweet Spots

- **Wins at:** disk-based posting lists where compression ratio matters more than random access, very simple to implement, great as a building block combined with block-level schemes
- **Loses at:** random access (must decode from start), intersection speed vs roaring

### At Our Scale

With 200M files and 30K-100K trigrams, average posting list length varies enormously (Zipf distribution). For common trigrams with millions of entries and average gap ~100-200, delta+varint achieves good compression. For rare trigrams with <100 entries, overhead is minimal. Simple to implement as a baseline.

---

## 3. PForDelta (Patched Frame of Reference)

### How It Works

1. Partition the delta-encoded sequence into fixed-size blocks (typically 128 or 256 integers)
2. For each block, find the bit width b that fits most values (e.g., the 90th percentile)
3. Encode all values using b bits each (frame of reference)
4. "Exceptions" (values that don't fit in b bits) are patched: stored separately at the end of the block with their positions

This avoids the problem of plain FOR where one outlier inflates the bit width for the entire block.

### Space Overhead vs Raw u32

- Typically 2-6 bits per integer for well-clustered data (vs 32 bits raw) = **6-19% of raw**
- Better than VByte for long lists
- Worse than VByte for very short lists (<128 elements) due to block overhead
- Comparable to or slightly worse than Elias-Fano for medium density

### Intersection Speed

- Decode is very fast: SIMD-friendly block decode (128 integers at once)
- S4-BP128-D4 (Lemire): decodes at ~0.7 CPU cycles per integer with SIMD
- After decode, standard merge/galloping intersection
- Lucene uses PFOR as its primary on-disk posting format and achieves competitive query times

### Build Speed

- Block-based encoding: O(n) with constant factor per block for finding optimal bit width
- More complex than delta+varint but still fast

### Sweet Spots

- **Wins at:** long posting lists on disk, SIMD-accelerated bulk decode, Lucene-scale search
- **Loses at:** short posting lists (<128 elements), in-memory random access patterns
- PForDelta is a disk format champion; for in-memory operations roaring is generally preferred

### At Our Scale

PForDelta is the right choice for on-disk storage of medium-to-long posting lists. For our trigram index, the most common trigrams (e.g., "the", "ing") will have posting lists with millions of entries where PForDelta shines. ClickHouse recently adopted PFOR + delta encoding for on-disk posting lists, reporting ~30% reduction vs roaring on disk.

---

## 4. SIMD Intersection

### Lemire's Work

Daniel Lemire et al. developed vectorized algorithms for sorted integer list intersection:

**Key paper:** "SIMD Compression and the Intersection of Sorted Integers" (2014, Software: Practice and Experience)

### Algorithms

1. **V1 (SIMD Merge):** Compare 4 pairs of integers simultaneously using `_mm_cmpestrm`. Process sorted arrays in lockstep, advancing the smaller side.

2. **SIMD Galloping:** When cardinality ratio is high (one list much smaller), use SIMD to gallop through the larger list. Each SIMD comparison checks 4 elements at once.

3. **S4-BP128-D4:** Combined compression + intersection scheme. Decodes 128 delta-encoded, bit-packed integers at a time using SIMD, then intersects.

### Performance

- SIMD intersection of two sorted arrays: ~2-4 cycles per comparison
- vs scalar galloping: ~6-10 cycles per comparison
- For compressed lists (S4-BP128-D4): decode + intersect at ~1.5 cycles per integer total
- CRoaring uses SIMD for array container intersection (arrays of u16, using SSE/AVX shuffles)

### Practical Considerations

- Requires SSE4.1+ (available on all x86-64 since ~2008)
- AVX2 doubles throughput (8 pairs at once)
- ARM NEON support available in CRoaring
- SIMD intersection benefits most when both lists are of similar size

### At Our Scale

SIMD intersection is relevant primarily through CRoaring (which already uses it for array container operations) or if we implement our own sorted-array posting format. For trigram queries, we typically intersect 2-4 posting lists, so fast intersection directly impacts query latency.

---

## 5. Elias-Fano Encoding

### How It Works

Quasi-succinct representation for monotone non-decreasing sequences. Given n integers from universe [0, m):

1. Split each integer into upper bits (top ceil(log(n)) bits) and lower bits (remaining ceil(log(m/n)) bits)
2. Store lower bits explicitly and contiguously (n * ceil(log(m/n)) bits total)
3. Store upper bits as a unary-coded sequence in a bitvector (2n bits)

### Space

Uses at most **2 + ceil(log(m/n))** bits per element, which is less than half a bit from the information-theoretic lower bound (hence "quasi-succinct").

| Universe / Count | Bits/element | vs Raw u32 |
|-----------------|-------------|-----------|
| 1B docs / 10M entries (gap ~100) | ~8.6 bits | 27% |
| 1B docs / 100M entries (gap ~10) | ~5.3 bits | 17% |
| 200M docs / 1M entries (gap ~200) | ~9.6 bits | 30% |
| 200M docs / 100K entries (gap ~2000) | ~13 bits | 41% |

### Key Properties

- **Random access in O(1) amortized time** via select operations on the upper-bits bitvector
- **NextGEQ (skip-to) in O(1) average time** using skip pointers -- critical for posting list intersection
- **Sequential scan** uses very few logical operations per element
- No decompression needed for access -- values can be read directly from the encoded form

### Intersection Speed

- Extremely fast for conjunctive queries due to O(1) NextGEQ operation
- Vigna's quasi-succinct indices showed competitive performance with PForDelta for conjunctive and phrasal queries
- Partitioned Elias-Fano (Ottaviano & Venturini 2014) improves compression by partitioning into chunks that exploit local clustering, with minimal time overhead

### Build Speed

- Single-pass construction: O(n) time
- Requires knowing n and upper bound m in advance (or two-pass)
- Slightly more complex than delta+varint but straightforward

### Sweet Spots

- **Wins at:** random access patterns, NextGEQ-heavy workloads (search engine intersection), memory-mapped on-disk indexes, theoretically optimal compression
- **Loses at:** very dense sequences (where plain bitmaps are better), sequential-only access (where simpler schemes are faster to decode)
- Partitioned Elias-Fano is the best known space/time tradeoff for inverted index compression

### At Our Scale

For our 200M file universe, Elias-Fano would use ~9-13 bits per entry for typical posting lists. The O(1) NextGEQ is extremely valuable for multi-term intersection queries. This is a strong candidate for on-disk posting list format, especially for medium-density lists.

---

## 6. How Lucene and Tantivy Store Posting Lists

### Lucene (since 4.1)

**On-disk format:** Frame of Reference (FOR) encoding with delta encoding.

- Posting lists split into **blocks of 128 doc IDs**
- Within each block: delta-encode, then bit-pack at the minimum required bit width
- Block header stores the bit width (1 byte)
- Term frequencies encoded similarly in interleaved blocks
- Skip lists layered on top for fast seeking (skip every N blocks)
- Recent enhancement (2025): dense blocks of 128 docs can be encoded as bit sets instead of delta lists when the bitset is smaller

**Filter cache (in-memory):** Roaring bitmaps (since Lucene 5, replacing plain FixedBitSet).

**Key insight from Elastic's blog:** On disk, FOR-encoded posting lists are competitive with in-memory representations because of OS page cache. The filter cache uses roaring bitmaps because it needs fast boolean operations, not just sequential scan.

### Tantivy (Rust search engine)

**On-disk format:** Delta encoding + bit-packing in blocks of 128, directly inspired by Lucene.

- Doc IDs and term frequencies interlaced in blocks of 128
- Delta-encode doc IDs, then bit-pack at minimum bit width
- Term frequencies bit-packed (no delta, since they aren't sorted)
- Last (partial) block uses variable-length integers instead of bit-packing
- Originally delegated to `simdcomp` (Lemire's C library) for SIMD bit-packing; now has pure Rust fallback via the `bitpacking` crate
- Skip data stored alongside for efficient seeking

**Key design points:**
- Segment-based architecture (immutable segments, merged in background)
- Each segment has its own term dictionary (FST-based) and posting lists
- Very similar to Lucene's approach but in Rust

---

## 7. How ClickHouse Stores Inverted Index Posting Lists

### Architecture

ClickHouse's full-text search (rebuilt from scratch, GA as of March 2026) uses:

**Dictionary:** Finite State Transducer (FST) per segment, with shared prefix/suffix compression. FSTs map tokens to byte offsets in the posting list file. Now Zstd-compressed on disk (10% size reduction).

**Posting lists:** Two representations depending on context:

1. **On disk:** PFOR (Patched Frame of Reference) + delta encoding. This was a recent change that reduced posting list storage by ~30% compared to their prior approach. The rationale: fast intersection/union is only needed in memory; on disk, compression ratio matters more.

2. **In memory (during query):** Roaring bitmaps. Posting lists are decoded into roaring bitmaps for fast boolean operations (AND, OR, NOT) during query execution.

**Pre-filter:** Bloom filters per segment to quickly rule out tokens before touching FST/posting data. Bloom filters are small enough to stay resident in memory.

**Segmentation:** Index split into segments (not by row count but by dictionary size) to control memory during index creation. Each segment is self-contained with its own FST + posting lists.

**Key innovation:** Row-level filtering directly from the index without reading the text column. This avoids the bottleneck of loading large text columns when the index already contains the match information.

### Layout on Disk

Five files per index:
1. ID file (segment ID + version)
2. Metadata file (segment offsets)
3. Bloom filter file
4. Dictionary file (FSTs, Zstd-compressed)
5. Posting list file (PFOR + delta encoded)

---

## 8. Comparison: Sorted Array vs Roaring Bitmap vs Delta-Encoded

### Space (bits per element)

| Cardinality | Universe | Sorted u32 | Delta+VByte | PForDelta | Roaring | Elias-Fano |
|------------|----------|-----------|------------|----------|---------|-----------|
| 100 | 200M | 32 | ~18 | ~24 (block overhead) | ~18 (array container) | ~29 |
| 10K | 200M | 32 | ~14 | ~8 | ~16 (array containers) | ~16 |
| 100K | 200M | 32 | ~11 | ~6 | ~16 (mostly array) | ~13 |
| 1M | 200M | 32 | ~8 | ~4 | ~8 (mix array+bitmap) | ~10 |
| 10M | 200M | 32 | ~5 | ~3 | ~3 (mostly bitmap) | ~6 |
| 100M | 200M | 32 | ~2 | ~1.5 | ~1 (bitmap) | ~3 |

*Values are approximate; actual results depend on data distribution.*

### Intersection Speed (relative, lower is faster)

| Approach | Sequential Scan | Random Skip (NextGEQ) | Boolean Ops (AND/OR) |
|----------|----------------|----------------------|---------------------|
| Sorted u32 | 1x (baseline) | Binary search | Manual merge |
| Delta+VByte | 1.5x (decode overhead) | Needs skip list | Decode + merge |
| PForDelta | 0.8x (SIMD bulk decode) | Good with skip list | Decode + merge |
| Roaring | 1x (container iteration) | Fast (container addressing) | Native, fastest |
| Elias-Fano | 1.2x | O(1) NextGEQ | Via NextGEQ |

### Build Speed (relative)

| Approach | Build Time | Complexity |
|----------|-----------|-----------|
| Sorted u32 | 1x (just sort) | Trivial |
| Delta+VByte | 1.1x | Very simple |
| PForDelta | 1.5x | Moderate (block analysis) |
| Roaring | 2x (container management) | Moderate |
| Elias-Fano | 1.2x | Simple (but needs n, max upfront) |

---

## 9. Recommendations for TurboDB

### Scale Analysis

- **Universe:** 60K-200M file IDs (u32)
- **Vocabulary:** 30K-100K unique trigrams
- **Distribution:** Zipf-like. Common trigrams ("the", "ing", " e ") appear in most files. Rare trigrams appear in <100 files.
- **Primary operation:** Intersect 2-5 posting lists for multi-trigram queries

### Recommended Architecture

**On-disk format: Delta + bit-packed blocks (FOR/PFOR style)**
- Use blocks of 128 u32 delta-encoded values, bit-packed at minimum width
- For blocks < 128 (last block or short lists), fall back to delta + varint
- This matches Lucene/Tantivy's proven approach
- Simple to implement, excellent compression, SIMD-friendly decode
- Can add PFOR patching later if outliers are a problem

**In-memory format: Roaring bitmaps for intersection**
- Decode posting lists into roaring bitmaps for boolean operations
- CRoaring available via roaring-zig bindings or the new pure-Zig zroaring
- Handles all density levels automatically
- Native AND/OR/NOT operations

**Alternative: Elias-Fano for on-disk if random access is critical**
- If we need NextGEQ without full block decode, Elias-Fano is superior
- Partitioned Elias-Fano gives best known compression/speed tradeoff
- More complex to implement but theoretically optimal

### Implementation Priority

1. **Phase 1 (MVP):** Sorted u32 arrays on disk, linear merge for intersection. Simple, correct, profile-able.
2. **Phase 2 (Compression):** Delta + bit-packing in blocks of 128. Reduces disk I/O significantly.
3. **Phase 3 (Fast intersection):** Roaring bitmaps in memory for boolean ops. Or SIMD-accelerated sorted array intersection via CRoaring.
4. **Phase 4 (Optimize):** Profile and decide between:
   - PFOR for better compression on long lists
   - Elias-Fano for O(1) skip-to on disk
   - Hybrid approach (different encodings for different list lengths, like ClickHouse)

### Key Numbers to Remember

- Roaring array container: 2 bytes/value (sparse), switches to bitmap at 4096 values/chunk
- Delta+bitpack: typically 4-10 bits/value for well-clustered data
- Elias-Fano: 2 + log(universe/count) bits/value, with O(1) random access
- SIMD decode (S4-BP128): ~0.7 cycles per integer
- SIMD intersection: ~2-4 cycles per comparison pair
- CRoaring intersection: often <0.5 cycles per element for bitmap containers

### Zig-Specific Resources

- **roaring-zig** (github.com/jwhear/roaring-zig): Zig bindings for CRoaring, mature
- **zroaring** (ziggit.dev, March 2026): Pure Zig roaring bitmap implementation, new
- **bitpacking:** Would need a Zig implementation or port of Lemire's simdcomp
- **Elias-Fano:** No known Zig implementation; would need to build (relatively straightforward, mainly bit manipulation + select/rank on bitvectors)
