# Parallel/Concurrent Indexing Techniques for Zig

Research date: 2026-03-27

---

## 1. Zig Thread Pool Patterns (std.Thread.Pool)

### How It Works

Zig 0.15's `std.Thread.Pool` provides a work-stealing thread pool in the standard library. The pattern:

```zig
var pool: std.Thread.Pool = undefined;
try pool.init(.{ .allocator = allocator, .n_jobs = num_cpus });
defer pool.deinit();

var wg: std.Thread.WaitGroup = .{};
for (files) |file| {
    pool.spawnWg(&wg, indexFile, .{ file, &shared_state });
}
wg.wait();
```

Each file gets spawned as a task. The pool distributes work across N threads. `WaitGroup` blocks until all tasks complete.

### Applicability

**Direct fit.** This is the primary mechanism for parallelizing file indexing. Each file's trigram extraction is independent CPU work — the ideal case for thread pools. For our codebase, the pattern would be: walk the file tree, then spawn one pool task per file (or per batch of files) to extract trigrams.

### Expected Speedup

- **2-4x on 4-8 cores** for CPU-bound trigram extraction (per ripgrep benchmarks, see below).
- Diminishing returns beyond ~8 threads due to Amdahl's law and memory bus contention.
- If I/O-bound (cold cache), parallelism helps less but still provides ~1.5-2x by overlapping I/O waits with CPU work.

### Complexity

**Low.** std.Thread.Pool is stable in 0.15. The main challenge is safely aggregating results from worker threads back into the shared index.

### Zig 0.15 Concerns

- `std.Thread.Pool` API is stable in 0.15.2. Uses `pool.init()` / `pool.deinit()` / `pool.spawnWg()`.
- No built-in work-stealing across spawn calls — each `spawnWg` is a discrete task.
- Pool threads share the process address space; must use Mutex or atomic operations for shared state.

### Sources

- https://www.mtsoukalos.eu/2026/01/thread-pool-in-zig/
- https://www.bradcypert.com/multithreading-zig/
- https://cookbook.ziglang.cc/07-03-threadpool/
- https://zig.news/kprotty/resource-efficient-thread-pools-with-zig-3291

---

## 2. Lock-Free / Sharded Hash Maps for Concurrent Indexing

### How It Works

Two main approaches for concurrent hash maps:

**a) Sharded HashMap (lock striping):**
Split the hash map into N shards, each protected by its own RwLock. Route keys to shards via `hash(key) % N`. Operations on different shards proceed in parallel. Read-heavy workloads benefit from RwLock allowing concurrent readers within the same shard.

```zig
const ShardedMap = struct {
    shards: [NUM_SHARDS]struct {
        lock: std.Thread.RwLock,
        map: std.HashMap(...),
    },

    fn getShard(self: *@This(), key: []const u8) *Shard {
        const h = std.hash.Wyhash.hash(0, key);
        return &self.shards[h % NUM_SHARDS];
    }
};
```

**b) Lock-free hash maps:**
No pure-Zig lock-free hash map exists in the standard library. The stdlib `HashMap` is not thread-safe. You would need to port a lock-free design (e.g., Harris-Michael linked list or a Cliff Click-style open-addressing map).

### Applicability

**Sharded approach is the practical choice.** For trigram indexing, the "key" is a trigram (3 bytes = 24 bits, so ~16M possible keys). We can shard by trigram prefix. With 256 shards (first byte of trigram), contention drops to near-zero since worker threads processing different files will typically touch different trigram distributions.

### Expected Speedup

- Sharded map with 64-256 shards: **near-linear scaling** up to shard count, assuming uniform distribution.
- Key optimization: use `@alignOf` or manual padding to avoid false sharing between adjacent shards on the same cache line (64 bytes on x86).

### Complexity

- **Sharded: Medium.** Straightforward to implement but must handle cache-line padding and careful shard count tuning.
- **Lock-free: High.** Requires deep expertise in memory ordering (`@fence`, `@atomicRmw`). Not recommended unless sharded approach proves insufficient.

### Zig 0.15 Concerns

- `std.Thread.RwLock` is available and stable.
- `std.Thread.Mutex` uses futex on Linux — very fast for uncontended cases.
- Zig's `@atomicLoad`, `@atomicStore`, `@atomicRmw`, `@cmpxchgWeak` are all available for lock-free work if needed.
- No `CachePadded` equivalent in stdlib — must manually pad structs to 64-byte alignment.

### Sources

- https://bluuewhale.github.io/posts/concurrent-hashmap-designs/
- https://hashimcolombowala.com/p/lock-striping/
- https://algomaster.io/learn/concurrency-interview/design-concurrent-hashmap
- https://listcomp.com/zig/2025/11/01/zig-the-hash-map-types-maze
- https://github.com/karlseguin/cache.zig

---

## 3. Arena Allocator Patterns for Batch Indexing

### How It Works

Allocate all trigrams for a single file in one arena, process them, then bulk-insert into the global index. The arena is reset or freed after each file.

```zig
fn indexFile(file_path: []const u8, global_index: *GlobalIndex) void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    // Extract all trigrams into arena-backed storage
    const trigrams = extractTrigrams(alloc, file_content);

    // Sort trigrams (dedup / prepare for bulk insert)
    std.sort.sort(Trigram, trigrams, {}, lessThan);

    // Bulk insert into global index (single lock acquisition)
    global_index.bulkInsert(file_id, trigrams);
}
```

### Applicability

**Excellent fit.** Each file's trigram extraction is a short-lived, allocation-heavy operation — the exact use case arenas are designed for. Benefits:

1. **Zero per-allocation overhead** — arena just bumps a pointer.
2. **Single deallocation** — `arena.deinit()` frees everything at once, no fragmentation.
3. **Cache-friendly** — trigrams for one file are contiguous in memory.
4. **Thread-safe** — each thread gets its own arena, no locking needed for allocation.

### Expected Speedup

- **2-5x faster allocation** compared to general-purpose allocator (no free-list management).
- **Reduced fragmentation** — especially important when indexing thousands of files.
- Main speedup is indirect: less allocator contention between threads, better cache locality during trigram extraction.

### Complexity

**Low.** Arena allocators are idiomatic Zig. The pattern is well-established.

### Zig 0.15 Concerns

- `std.heap.ArenaAllocator` is stable and well-tested.
- Can be backed by `page_allocator` (mmap-based, good for large allocations) or a `FixedBufferAllocator` (for bounded memory).
- `arena.reset(.retain_capacity)` allows reuse without returning pages to the OS — useful when processing many files sequentially on one thread.

### Sources

- https://zig.guide/standard-library/allocators/
- https://www.openmymind.net/Leveraging-Zigs-Allocators/
- https://github.com/ziglang/zig/blob/master/lib/std/heap/arena_allocator.zig

---

## 4. Memory-Mapped Concurrent Writes (Database Techniques)

### How It Works

Databases like LMDB use memory-mapped files with MVCC (Multi-Version Concurrency Control) to allow concurrent reads during writes. Key techniques:

1. **MVCC**: Readers see a consistent snapshot while writers modify a separate copy. No reader locks needed.
2. **Copy-on-Write B-trees**: Modified pages are written to new locations; the root pointer is atomically swapped.
3. **Append-only logs**: New index entries are appended to a log; a background process merges them into the main index.
4. **Partition-then-merge**: Each thread builds its own local index shard, then shards are merged in a final pass.

### Applicability

**Partition-then-merge is the best fit for our use case.** Rather than concurrent writes to a shared mmap region (complex and OS-dependent), each worker thread builds a local trigram index in memory, then a merge pass combines them.

```
Phase 1 (parallel): N threads each build partial index for their file batch
Phase 2 (serial or tree-merge): Merge N partial indexes into final index
```

This avoids all write contention during the CPU-intensive phase. The merge is a simple sorted-list merge operation.

For the final on-disk format, mmap for reads is excellent:
- `std.posix.mmap()` maps the index file into memory.
- Multiple search threads can read concurrently with zero copying.
- OS page cache handles caching automatically.

### Expected Speedup

- Phase 1 scales linearly with cores (no contention).
- Phase 2 (merge) is O(total_trigrams * log(N_shards)) — fast for small N.
- Net: **3-6x on 8 cores** compared to single-threaded.

### Complexity

**Medium.** The partition phase is trivial. The merge phase requires careful design of the on-disk format to support efficient merging.

### Zig 0.15 Concerns

- `std.posix.mmap` / `std.posix.munmap` available on POSIX systems.
- On macOS, `MAP_SHARED` writes from multiple threads to the same mmap region are undefined behavior without coordination — reinforces the partition-then-merge approach.
- Windows support requires `std.os.windows.VirtualAlloc` or similar — less portable.

### Sources

- https://medium.com/@akp83540/lightning-memory-mapped-database-lmdb-8b9e2c05d525
- https://vldb.org/pvldb/vol17/p3442-hao.pdf (Bf-Tree)
- https://turso.tech/blog/beyond-the-single-writer-limitation-with-tursos-concurrent-writes

---

## 5. Ripgrep's Parallelism Model

### How It Works

Ripgrep uses a **parallel directory walker** combined with a **thread pool for searching**:

1. **Parallel directory traversal**: The `ignore` crate walks the filesystem using multiple threads. Each thread takes a directory, lists its contents, and pushes subdirectories to a shared work-stealing deque.

2. **Per-file search parallelism**: Each thread picks files from the shared queue and searches them independently. Results are buffered per-file to prevent interleaved output.

3. **Gitignore parallelism**: The construction and matching of gitignore rules is itself parallelized across threads.

4. **Output buffering**: Each file's results are buffered entirely in memory before being written to stdout, preventing interleaving.

### Key Findings from BurntSushi's Benchmarks

On the Chromium source tree (~394K files):
- **Pure traversal** (`-uuu --files`): Optimal at 4 threads (0.163s vs 0.255s single-threaded = 1.6x).
- **Traversal + filtering** (`--files`): Optimal at 8 threads (0.214s vs 0.701s single-threaded = 3.3x).
- Beyond optimal thread count, performance degrades due to coordination overhead (Amdahl's law).
- The more CPU work per file (e.g., gitignore matching), the more parallelism helps.

### Applicability

**Directly applicable architecture.** Our indexing pipeline has significant CPU work per file (trigram extraction, symbol parsing), which means we should see even better parallel scaling than ripgrep's file-listing benchmark. Key takeaways:

1. Use a **work-stealing queue** for file distribution (or just batch files evenly across threads).
2. **Buffer results per-file** before merging into the global index.
3. **4-8 threads is the sweet spot** for most workloads on modern hardware.
4. The per-file CPU work (trigram extraction) is heavier than ripgrep's search, so scaling should be better.

### Expected Speedup

- **3-4x** on 8 cores for trigram indexing (more CPU-bound than ripgrep's search).
- Could reach **5-6x** if combined with arena allocation and batched index insertion.

### Complexity

**Low-Medium.** The work-stealing queue is provided by `std.Thread.Pool`. The main design work is in result aggregation.

### Zig 0.15 Concerns

- Zig's `std.Thread.Pool` provides similar semantics to Rust's rayon/crossbeam thread pools.
- No built-in parallel directory walker — would need to implement or use single-threaded walk + parallel processing.
- `std.fs.Dir.iterate()` is not thread-safe; must walk on a single thread then distribute files.

### Sources

- https://github.com/BurntSushi/ripgrep/discussions/2472
- https://burntsushi.net/ripgrep/
- https://dev.to/amartyajha/why-ripgrep-rg-beats-grep-for-modern-code-search-5-deep-technical-reasons-j76

---

## 6. Zoekt's Trigram Index — Parallel Shard Building

### How It Works

Zoekt (Sourcegraph's code search) uses a **shard-based architecture**:

1. **Positional trigram index**: Every 3-byte sequence is indexed with byte offsets. Queries look up trigrams and verify they appear at correct distances.

2. **Shard files** (`.zoekt`): Each shard is a memory-mappable file limited to ~4GB (uint32 offsets). Large repos are split across multiple shards.

3. **Parallel search across shards**: Each shard is searched by a single goroutine. Parallelism = number of shards.

4. **Index pipeline**: `gitindex -> Builder -> ShardBuilder -> .zoekt` file. The Builder processes documents and builds posting lists. The ShardBuilder serializes to disk.

5. **Compound shards**: Small repo shards can be merged into compound shards for efficiency, then exploded back if needed.

6. **Atomic updates**: New shards are written atomically; the search server watches for filesystem changes and hot-reloads.

### Applicability

**Key architectural lessons for our codebase:**

1. **Shard by repository** (or directory subtree): Each shard is independently buildable. Multiple shards can be built in parallel on different threads.

2. **Memory-map for search**: Shards are mmap'd at search time — zero deserialization cost, OS handles page caching.

3. **uint32 offsets**: Limits shard size but enables compact, fast index format. Our trigram posting lists should use uint32 file IDs.

4. **Compound shard merging**: After initial parallel build, small shards can be merged to reduce file count and improve search locality.

5. **Decouple indexing from search**: Build the index as a separate pass, write atomic shard files, let the search system hot-reload.

### Expected Speedup

- Shard-parallel build: **Linear with number of shards** (each shard built on its own thread).
- For a 10K-file codebase split into 8 shards: ~8x parallel build speedup.
- Search parallelism also scales with shard count.

### Complexity

**Medium-High.** Requires designing a shard file format, shard splitting strategy, and atomic reload mechanism. The indexing pipeline itself is straightforward per-shard.

### Zig 0.15 Concerns

- Zig can produce compact binary formats with `packed struct` — ideal for shard headers.
- `@ptrCast` and pointer arithmetic for reading mmap'd shard data must be carefully aligned.
- No filesystem watcher in std — would need platform-specific code (kqueue on macOS, inotify on Linux) or polling.

### Sources

- https://deepwiki.com/sourcegraph/zoekt/1.1-architecture
- https://deepwiki.com/sourcegraph/zoekt/4-indexing-system
- https://github.com/sourcegraph/zoekt/blob/main/doc/design.md

---

## 7. Roaring Bitmap Implementations for Compact Posting Lists

### How It Works

Roaring bitmaps store large sets of 32-bit integers with excellent compression and fast set operations. The structure:

1. **Top-level**: Array of containers, indexed by the high 16 bits of each integer.
2. **Per-container** (determined by cardinality):
   - **Array container**: Sorted array of uint16 values (for sparse sets, < 4096 elements).
   - **Bitmap container**: 8KB bitset (for dense sets, >= 4096 elements).
   - **Run container**: RLE-encoded runs (for consecutive ranges).

Set operations (AND, OR, XOR) dispatch to optimized routines based on container type pairs. CRoaring uses SIMD (AVX2, AVX-512, NEON) for bitmap-bitmap operations.

### Zig Implementations Available

1. **Rawr** (pure Zig, Feb 2026): 100% interoperable with CRoaring format. Performance comparable to C implementation. No libc dependency. https://github.com/awesomo4000/rawr

2. **Zroaring** (Zig port, Mar 2026): Zig port of CRoaring. Early stage, actively developed. Uses tagged pointers for container type discrimination. https://codeberg.org/archaistvolts/zroaring

3. **roaring-zig** (C bindings): Zig bindings for CRoaring. Gets SIMD optimizations from the C library but requires C compilation.

### Applicability

**Good fit for posting lists if file count exceeds ~1000.** For trigram indexing:

- Each trigram maps to a set of file IDs (the "posting list").
- With Roaring bitmaps, posting list storage is compressed and set intersection (for multi-trigram queries) is hardware-accelerated.
- For small codebases (< 1000 files), sorted uint32 arrays may be simpler and equally fast.

**Trade-off analysis:**

| Approach | Storage per posting list | Intersection speed | Complexity |
|---|---|---|---|
| Sorted uint32 array | 4 bytes/entry | O(n+m) merge | Low |
| Bitset (fixed) | file_count/8 bytes | O(n) AND | Low |
| Roaring bitmap | ~0.5-2 bytes/entry | O(n) SIMD | Medium |

For a 100K-file codebase with ~16M trigrams, Roaring could save 50-80% memory on posting lists compared to sorted arrays, and intersection is 2-10x faster due to SIMD.

### Expected Speedup

- **Memory reduction**: 50-80% for posting lists (compared to raw uint32 arrays).
- **Query intersection**: 2-10x faster with SIMD, especially for multi-term queries.
- **Build time**: Slightly slower than appending to arrays (container management overhead), but negligible compared to I/O.

### Complexity

- **Using Rawr/Zroaring**: **Low-Medium.** Drop-in library, mature format.
- **Custom implementation**: **High.** Many container type combinations, SIMD optimization required.
- **Recommendation**: Start with Rawr (pure Zig, no libc, good performance), fall back to CRoaring bindings if SIMD performance is critical.

### Zig 0.15 Concerns

- Rawr and Zroaring are both targeting recent Zig versions and should work on 0.15.
- Zig's `@Vector` type can be used for SIMD if writing custom bitmap operations.
- `packed struct` and bit manipulation are first-class in Zig — good for bitmap containers.
- CRoaring C bindings via `@cImport` work but add build complexity.

### Sources

- https://github.com/awesomo4000/rawr
- https://codeberg.org/archaistvolts/zroaring
- https://github.com/jwhear/roaring-zig
- https://github.com/RoaringBitmap/CRoaring
- https://arxiv.org/abs/1709.07821

---

## Recommended Architecture: Putting It All Together

Based on this research, here is the recommended parallel indexing pipeline:

```
┌─────────────────────────────────────────────────────────┐
│                    File Discovery                        │
│  Single-threaded directory walk → file list              │
│  (std.fs.Dir.iterate, fast, I/O-bound)                  │
└──────────────────────┬──────────────────────────────────┘
                       │ file list
                       ▼
┌─────────────────────────────────────────────────────────┐
│              Parallel Trigram Extraction                  │
│  std.Thread.Pool (4-8 workers)                          │
│  Each worker:                                            │
│    1. Arena allocator per file (or per batch)            │
│    2. Read file content                                  │
│    3. Extract trigrams → arena-backed sorted array       │
│    4. Build local partial index (thread-local HashMap)   │
│    5. Arena reset after each file                        │
└──────────────────────┬──────────────────────────────────┘
                       │ N partial indexes
                       ▼
┌─────────────────────────────────────────────────────────┐
│                   Merge Phase                            │
│  Tree merge of N partial indexes → global index          │
│  Posting lists: sorted uint32 arrays or Roaring bitmaps │
│  (serial or parallel pairwise merge)                     │
└──────────────────────┬──────────────────────────────────┘
                       │ final index
                       ▼
┌─────────────────────────────────────────────────────────┐
│                Shard Serialization                        │
│  Write index to mmap-friendly binary format              │
│  Optional: split into shards for parallel search         │
│  Atomic file replacement for hot reload                  │
└─────────────────────────────────────────────────────────┘
```

### Priority Order for Implementation

| Priority | Technique | Speedup | Effort | Risk |
|---|---|---|---|---|
| **P0** | Thread pool + per-file parallelism | 3-4x | Low | Low |
| **P0** | Arena allocator per file | 2-5x alloc speed | Low | None |
| **P1** | Partition-then-merge (thread-local indexes) | Eliminates contention | Medium | Low |
| **P1** | Sharded HashMap for global index | Near-linear scaling | Medium | Low |
| **P2** | Roaring bitmaps for posting lists | 50-80% memory savings | Medium | Low |
| **P3** | Shard-based index files (Zoekt-style) | Parallel search | High | Medium |
| **P3** | mmap-based index serving | Zero-copy search | Medium | Medium |
