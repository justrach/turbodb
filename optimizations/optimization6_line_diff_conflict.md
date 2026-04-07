# Optimization 6: Line-Level Diff + Real Conflict Detection

**Date:** 2026-04-06
**Components:** `src/branch.zig`, `src/collection.zig`, `src/ffi.zig`
**Issue:** #47

---

## What was broken

`mergeBranch` always fast-forwarded. Two agents editing the same file → second merge silently overwrites the first. Data integrity bug.

Also: diffs were per-file only. You could see "auth.zig was modified" but not which lines changed.

## What was fixed

### 1. Per-key epoch tracking (`src/collection.zig`)

Added `key_epochs: AutoHashMap(u64, u64)` to Collection. Every `insert()` records `key_epochs.put(key_hash, doc_id)` where doc_id is the monotonic epoch.

On merge, for each branch write:
```zig
if (self.key_epochs.get(key_hash)) |main_epoch| {
    if (main_epoch > br.base_epoch) {
        // Main modified this key after branch was created → CONFLICT
    }
}
```

**3 lines of logic. Fixes a data integrity bug.**

### 2. Line-level diff (`src/branch.zig` L216-375)

`lineDiff(old, new, alloc)` — computes line-by-line diff entirely in Zig.

Algorithm: split both strings into lines, walk forward with greedy matching. On mismatch, look ahead up to 5 lines in both directions to find the resync point. Unmatched lines are marked as added/removed.

```zig
pub const LineDiff = struct {
    line_no: u32,
    kind: enum(u8) { same, added, removed },
    text: []const u8,
};
```

FFI: `turbodb_branch_diff` returns JSON:
```json
{"files": [{"key": "src/auth.zig", "lines": [
    {"no": 1, "kind": "removed", "text": "pub fn auth() bool {"},
    {"no": 1, "kind": "added",   "text": "pub fn auth(token: []u8) bool {"},
    {"no": 2, "kind": "removed", "text": "    return false;"},
    {"no": 2, "kind": "added",   "text": "    return verify(token);"},
    {"no": 3, "kind": "same",    "text": "}"}
]}]}
```

---

## Full Branch API (6 calls)

```python
col.create_branch("fix-auth", "agent-42")          # create
col.branch_write("fix-auth", "src/auth.zig", code)  # write
col.branch_read("fix-auth", "src/auth.zig")         # read (CoW)
col.branch_diff("fix-auth")                         # line-level diff
col.branch_merge("fix-auth")                        # merge (conflict detection)
col.compare_branches("agent-a", "agent-b")          # side-by-side review
```

Plus discovery + search:
```python
col.branch_search("fix-auth", "authenticate")       # search on branch
col.discover_context("authenticate")                 # find files + callers + tests
col.list_branches()                                  # list all branches
```

---

## Files Changed

| File | Lines | Description |
|---|---|---|
| `src/branch.zig` | +164 | `LineDiff` struct, `lineDiff()` algorithm, `splitLines()`, 2 tests |
| `src/collection.zig` | +5 | `key_epochs` field, init, deinit, put on insert, check on merge |
| `src/ffi.zig` | +70 | `turbodb_branch_diff` FFI — computes diff in Zig, returns JSON |
| `python/turbodb/__init__.py` | +23 | `branch_diff()` method |
| `python/turbodb/_ffi.py` | +3 | ctypes prototype |
