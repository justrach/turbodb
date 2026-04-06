# Optimization 5: Code Layer — Git-like Branching for Agent Swarms

**Date:** 2026-04-06
**Components:** `src/branch.zig` (new), `src/collection.zig`, `src/server.zig`, `src/ffi.zig`
**Impact:** Zero-overhead branching with CoW isolation for multi-agent code editing
**Issue:** #44, **PR:** #45

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Agent Swarm                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ Agent A   │  │ Agent B   │  │ Agent C   │              │
│  │ fix-auth  │  │ add-logs  │  │ refactor  │              │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘              │
│        │              │              │                   │
│   Python FFI    Python FFI    Python FFI                 │
│   or HTTP       or HTTP       or HTTP                    │
└────────┼──────────────┼──────────────┼───────────────────┘
         │              │              │
┌────────▼──────────────▼──────────────▼───────────────────┐
│                  TurboDB Code Layer                       │
│                                                          │
│  ┌─────────────────────────────────────────────────┐     │
│  │              BranchManager                       │     │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐        │     │
│  │  │ Branch A │ │ Branch B │ │ Branch C │        │     │
│  │  │ writes:  │ │ writes:  │ │ writes:  │        │     │
│  │  │ auth.zig │ │ log.zig  │ │ util.zig │        │     │
│  │  │ (delta)  │ │ (delta)  │ │ (delta)  │        │     │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘        │     │
│  │       │             │             │              │     │
│  │       └─────────────┼─────────────┘              │     │
│  │                     │ CoW fallthrough             │     │
│  └─────────────────────┼────────────────────────────┘     │
│                        ▼                                  │
│  ┌─────────────────────────────────────────────────┐     │
│  │              Collection (main)                   │     │
│  │  PageFile + BTree + TrigramIndex + VectorColumn  │     │
│  │  WAL + MVCC + CDC + Epochs                       │     │
│  └─────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────┘
```

## Communication Protocols

### FFI (Fast Path — In-Process)
Used when agent runs in the same process or via Python ctypes:

```
Python/Zig process
  → ctypes call to libturbodb.dylib
  → Direct memory access to Collection struct
  → Zero serialization overhead
  → ~100ns per call
```

**5 FFI exports for branching:**
| Function | Args | Returns | Purpose |
|---|---|---|---|
| `turbodb_enable_branching` | col | 0/-1 | Lazy-init BranchManager |
| `turbodb_create_branch` | col, name, agent_id | 0/-1 | Create CoW branch |
| `turbodb_branch_write` | col, branch, key, value | 0/-1 | Isolated write |
| `turbodb_branch_read` | col, branch, key | value ptr + len | CoW read |
| `turbodb_branch_merge` | col, branch | 0 or conflict count | Merge to main |
| `turbodb_branch_search` | col, branch, query, limit | ScanHandle | Search on branch |
| `turbodb_list_branches` | col | JSON array | List all branches |

### HTTP (Remote Path — Network)
Used when agent runs in a turbobox sandbox or remote machine:

```
Agent in sandbox
  → HTTP request to TurboDB server
  → Server dispatches to Collection method
  → JSON response
  → ~1-5ms per call (network + serialization)
```

**6 HTTP endpoints:**
| Method | Path | Body | Response | Purpose |
|---|---|---|---|---|
| POST | `/branch/:col` | `{"name":"...","agent_id":"..."}` | `{"created":true}` | Create branch |
| GET | `/branch/:col` | — | `["branch1","branch2"]` | List branches |
| PUT | `/branch/:col/:branch/:key` | raw file content | `{"written":true}` | Write on branch |
| GET | `/branch/:col/:branch/:key` | — | raw file content | Read (CoW) |
| POST | `/branch/:col/:branch/merge` | — | `{"merged":true}` or 409 | Merge to main |
| GET | `/branch/:col/:branch/search?q=...` | — | `[{docs}]` | Search on branch |

### Comparison: Git vs TurboDB Communication

| Aspect | Git | TurboDB |
|---|---|---|
| **Local read** | `read()` on `.git/objects/` | FFI call → mmap PageFile |
| **Local write** | `write()` to loose object | FFI call → WAL + PageFile append |
| **Branch create** | Write ref file (40 bytes) | Set epoch marker (8 bytes, O(1)) |
| **Branch switch** | Update HEAD + checkout files | No-op (branches are views) |
| **Diff** | Walk tree objects, compare blobs | Walk branch HashMap (only deltas) |
| **Merge** | 3-way blob merge per file | Per-key overwrite + conflict detect |
| **Remote push** | Pack objects → send over SSH/HTTP | branch.merge() → CDC webhook |
| **Search** | Shell out to `grep` or external tool | Native trigram + vector index |
| **Remote clone** | Download entire packfile | N/A (agents get a branch view, not a copy) |

---

## Data Model

### Branch struct (`src/branch.zig` L16-45)

```zig
pub const Branch = struct {
    name: [64]u8,              // branch name (e.g. "fix-auth")
    name_len: u8,
    base_epoch: u64,           // forked from main at this epoch
    created_at: i64,           // millisecond timestamp
    status: BranchStatus,      // active | merged | abandoned
    agent_id: [64]u8,          // which agent owns this (e.g. "agent-42")
    agent_id_len: u8,
    
    // CoW storage: ONLY modified keys (HashMap of deltas)
    writes: AutoHashMap(u64, BranchWrite),
    allocator: Allocator,
};
```

### BranchWrite struct (`src/branch.zig` L36-42)

```zig
pub const BranchWrite = struct {
    key: []const u8,      // owned copy of file path
    value: []const u8,    // owned copy of file content
    deleted: bool,        // true = tombstone
    epoch: u64,           // when this write happened
};
```

### BranchManager (`src/branch.zig` L130-185)

```zig
pub const BranchManager = struct {
    branches: StringHashMap(*Branch),  // name → Branch
    allocator: Allocator,
    next_epoch: *atomic.Value(u64),    // shared with Collection
};
```

### Integration in Collection (`src/collection.zig`)

```zig
// New field (lazy-initialized, null when unused):
branch_mgr: ?*branch_mod.BranchManager = null,
```

**Zero overhead:** When `branch_mgr` is null (default), no branch code runs. The field is a single pointer (8 bytes). All existing Collection methods (insert, get, search) are completely unchanged.

---

## Copy-on-Write Read Path

```
getOnBranch(branch, "src/auth.zig"):
  1. Check branch.writes HashMap for key_hash
     → Found? Return branch-local value (or null if tombstone)
     → Not found? Continue to step 2
  2. Fall through to main: self.get("src/auth.zig")
     → Returns the main collection's version of the file
```

**Memory:** Only modified keys consume storage. A branch with 3 modified files out of 10,000 stores exactly 3 entries in its HashMap — not 10,000.

---

## Merge Logic

```
mergeBranch(branch):
  For each key in branch.writes:
    If key exists on main:
      Fast-forward: overwrite main with branch value
      (Future: detect if main changed since branch.base_epoch → conflict)
    If key is new:
      Insert into main
    If key is deleted (tombstone):
      Count as applied (full delete support is future work)
  
  If no errors: branch.status = .merged
  If insert fails: add to conflicts list
  
  Return: {applied: N, conflicts: [...]}
```

**CDC on merge:** Each `self.insert()` call during merge triggers `emitChange()` → CDC webhook fires automatically. No additional code needed.

---

## Agent Swarm Workflow

```python
import turbodb

db = turbodb.Database("./repo-data")
col = db.collection("myproject")

# === Setup: write initial codebase ===
col.insert("src/main.zig", "pub fn main() void {}")
col.insert("src/auth.zig", "pub fn auth() bool { return false; }")

# === Agent A: fix authentication ===
col.create_branch("fix-auth", "agent-a")
col.branch_write("fix-auth", "src/auth.zig", 
    "pub fn auth(tok: []u8) bool { return verify(tok); }")
col.branch_write("fix-auth", "src/validator.zig",
    "pub fn verify(tok: []u8) bool { return tok.len > 0; }")

# Agent A can search on their branch:
results = col.branch_search("fix-auth", "verify")
# → finds both auth.zig (modified) and validator.zig (new)

# === Agent B: add logging (parallel) ===
col.create_branch("add-logging", "agent-b")
col.branch_write("add-logging", "src/logger.zig",
    "pub fn log(msg: []u8) void { std.debug.print(msg); }")

# === Both merge (no conflicts — different files) ===
col.branch_merge("fix-auth")     # → 0 conflicts
col.branch_merge("add-logging")  # → 0 conflicts

# === Main now has all changes ===
col.get("src/validator.zig")  # from agent A
col.get("src/logger.zig")     # from agent B
```

---

## Performance Characteristics

| Operation | Complexity | Latency |
|---|---|---|
| Create branch | O(1) | ~1µs (set epoch marker) |
| Write on branch | O(1) | ~100ns (HashMap insert) |
| Read on branch (hit) | O(1) | ~50ns (HashMap lookup) |
| Read on branch (miss) | O(log N) | ~1µs (B-tree lookup on main) |
| Diff branch | O(B) | B = branch writes, ~1µs per entry |
| Merge branch | O(B × log N) | B writes × insert into main |
| Search on branch | O(T + B) | T = trigram search + B = branch overlay |
| List branches | O(K) | K = number of branches |

**Comparison to Git:**
| Operation | Git | TurboDB |
|---|---|---|
| Branch create | ~100ms (write ref) | ~1µs |
| Checkout | O(N files) | O(1) (no checkout needed) |
| Diff | O(N blobs) | O(B deltas only) |
| Merge (no conflict) | O(N blobs) | O(B deltas only) |

---

## Files Changed

| File | Lines | Description |
|---|---|---|
| `src/branch.zig` | +230 (new) | Branch, BranchManager, DiffEntry, MergeResult, 4 tests |
| `src/collection.zig` | +120 | 8 new methods, branch_mgr field, containsInsensitive helper |
| `src/server.zig` | +130 | 6 HTTP endpoints + 6 handler functions |
| `src/ffi.zig` | +80 | 7 FFI exports |
| `python/turbodb/__init__.py` | +60 | 5 Python methods on Collection |
| `python/turbodb/_ffi.py` | +30 | ctypes prototypes |
| `build.zig` | +1 | branch.zig in test-all list |

---

## Prior Art Comparison

| Feature | Git | Dolt | Neon | TurboDB |
|---|---|---|---|---|
| Language | C | Go | Rust/C | Zig |
| Branch model | Ref pointers | SQL BRANCH | Page-level CoW | Key-level CoW |
| Storage | Packfiles | Prolly trees | Page store | PageFile + B-tree |
| Search | External grep | SQL queries | N/A | Native trigram + vector |
| Agent support | Worktrees (heavyweight) | N/A | N/A | First-class branches |
| Merge | Line-level 3-way | Row-level | N/A | Key-level (per-file) |
| Protocol | SSH/HTTP/Git | MySQL wire | Postgres wire | FFI + HTTP |

---

## Open Questions / Future Work

1. **Per-key epoch tracking** — currently merge always fast-forwards. Need to store `last_modified_epoch` per key on main to detect true conflicts (both sides modified same file since fork).

2. **Branch persistence** — currently branches are in-memory only. Need to persist branch state to PageFile for crash recovery.

3. **Rebase** — replay branch commits on top of new main head. Requires tracking individual write epochs within a branch.

4. **Branch-scoped vector search** — currently hybrid search doesn't scope to branch. Need to integrate VectorColumn with branch overlay.

5. **Garbage collection** — abandoned branches should be cleaned up. Need TTL or explicit GC pass.

6. **Authentication** — per-agent tokens with branch-level permissions. Agent A can't write to Agent B's branch.
