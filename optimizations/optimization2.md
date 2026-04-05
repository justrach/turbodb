# Optimization 2: File-ID Indirection + Batch Indexing

**Date:** 2026-04-01
**Component:** `src/codeindex.zig` (TrigramIndex) + `src/collection.zig` (indexWorker)
**Impact:** 1.4x over opt 1, 4.1x over original (95% recall: 45s → 15.7s → 10.9s at 50K docs)

---

## Problem

After Optimization 1, the trigram indexer still had significant overhead:
1. **~500 path string allocations per document** — each unique trigram duped the file path string for its posting list. At 50K docs with ~500 unique trigrams each = 25 million heap allocations.
2. **Lock acquired per document** — 50K lock/unlock cycles on the mutex.
3. **`removeFile()` called unconditionally** — wasted work for new files (the bulk insert case).
4. **Fresh `AutoHashMap` per document** — pass 1 allocated and freed a temporary hashmap every time.

## Changes Made

### 1. File-ID Indirection (`src/codeindex.zig`)

Replaced all string-keyed posting lists with integer-keyed ones:

| Before | After |
|---|---|
| `AutoHashMap(Trigram, StringHashMap(PostingMask))` | `AutoHashMap(Trigram, AutoHashMap(u32, PostingMask))` |
| `StringHashMap(ArrayList(Trigram))` (file_trigrams) | `AutoHashMap(u32, ArrayList(Trigram))` |
| Path duped ~501 times per doc | Path duped **once** per new file |

New fields on TrigramIndex:
- `path_to_id: StringHashMap(u32)` — resolve path to file_id
- `id_to_path: ArrayList([]const u8)` — O(1) file_id to path lookup
- `next_id: u32` — monotonic file_id counter

`candidates()` now iterates `AutoHashMap(u32, PostingMask)` and translates file_ids back to paths via `id_to_path.items[file_id]`.

### 2. Guard removeFile for new files

```zig
if (self.file_trigrams.contains(file_id)) {
    self.removeFileById(file_id);
}
```

Skips the entire removal loop for new files (the common case during bulk insert).

### 3. Batch indexing in worker (`src/collection.zig`)

`indexWorker` now batches up to 16 documents per lock acquisition:
- Pop up to 16 items from the queue
- `indexBatch()` runs pass 1 (trigram extraction) for all docs outside the lock
- Takes the lock once and inserts all 16 documents
- Reusable `AutoHashMap(Trigram, void)` allocated once, cleared between docs via `clearRetainingCapacity()`

### 4. Disk persistence updated

- `writeToDisk` reads file table directly from `id_to_path` (no temporary file table needed)
- `readFromDisk` rebuilds `path_to_id` and `id_to_path` from the file table
- Posting lists already used u32 file_ids on disk — format is compatible

---

## Results

### Recall curve at 50K Wikipedia articles

```
Time     Opt 1       Opt 2 (this)
----------------------------------
  0s         9%          13%
  1s        15%          25%
  2s        29%          31%
  3s        35%          42%
  5s        49%          62%
  8s        65%          80%
 10s        74%          99%
 15s        99%          99%
```

### Summary

| Metric | Original | Opt 1 | Opt 2 | Total improvement |
|---|---|---|---|---|
| 95% recall (50K docs) | ~45s | 15.7s | 10.9s | **4.1x faster** |
| Insert throughput | 70K/s | 72K/s | 79.6K/s | **14% faster** |
| Allocs per doc (indexer) | ~3,600 | ~500 | ~2 | **1,800x fewer** |
| Lock acquisitions (50K docs) | 50,000 | 50,000 | ~3,125 | **16x fewer** |

---

## Files Changed

| File | Changes |
|---|---|
| `src/codeindex.zig` | TrigramIndex struct (file-ID fields), `indexFile`, `indexBatch`, `removeFileById`, `candidates`, `candidatesRegex`, `deinit`, `writeToDisk`, `readFromDisk` |
| `src/collection.zig` | `indexWorker` batch loop with reusable hashmap |
