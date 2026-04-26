# AGENTS.md — turbodb architecture for contributors

This is a guide for humans and AI agents working on turbodb. It covers the
shape of the codebase, the invariants that matter, the test/stress
infrastructure, and the patterns to copy when adding features.

> If you're trying to make a behaviour change: read the **Invariants** section
> first. Most bugs in this codebase are not "wrong code", they're "code that
> ignored an invariant nobody wrote down". A few months ago none of them were
> written down — that's why this file exists.

---

## What turbodb is

A schemaless document database in Zig 0.16, MongoDB-shaped on the outside,
with optional ACID and TigerBeetle-style atomic batches on the inside.

- **Wire**: HTTP/JSON on `:27018`, binary protocol on `:27017`, FFI for
  Python/Node.
- **Storage**: B-tree primary key index + ART secondary index + LSM tier +
  WAL with group commit + mmap'd page allocator.
- **Concurrency**: per-key stripe locks (1024 stripes), MVCC version chains,
  background indexer threads.
- **Replication**: Calvin-style deterministic sequencer (separate from this
  document — see `src/replication/`).
- **Distribution**: cross-compiles to `aarch64-linux-musl`, runs in Apple
  `container` or Docker.

---

## File layout

```
src/
├── main.zig             entry point — parses args, brings up runtime + server
├── runtime.zig          process-wide std.Io instance
├── compat.zig           Zig 0.15 → 0.16 stdlib shims (FS, Thread.sleep, …)
├── collection.zig       THE engine: insert/update/delete/scan, applyTxn,
│                        nextEngineTs, stripe_locks, idx_mu, hash_idx,
│                        ts_by_doc_id, B-tree, MVCC versions
├── server.zig           HTTP dispatcher, query parameter parsing, schema
│                        validator, atomic txn handler
├── query_engine.zig     ?select / ?where / ?order / ?join post-processor
├── doc.zig              binary doc layout (32-byte header + key + value)
├── lsm.zig              LSM tree (memtable → L0 SSTable)
├── btree.zig, art.zig   primary + adaptive radix index
├── mvcc.zig             VersionChain for time-travel queries
├── storage/
│   ├── wal.zig          write-ahead log with group commit
│   ├── mmap.zig         page allocator
│   ├── parallel_wal.zig per-core WAL segments (test-only standalone)
│   └── epoch.zig        epoch-based GC
├── io_engine.zig        kqueue/io_uring event loop, connection pool
├── crypto.zig, auth.zig API key auth (BLAKE3 + ed25519)
└── replication/         Calvin replication (out of scope for this doc)

parity/
├── parity_test.py       same-shape financial data into TB + TD, compare
└── divergence.py        probe where TB and TD differ; auto-detects MATCH/DIVERGE

stress/
├── stress.sh            N workers × M iters HTTP hammer
├── throughput.sh        sustained throughput measurement
└── bulk_throughput.sh   bulk-endpoint variant

Containerfile            Alpine-based runtime; copies static aarch64-linux-musl binary
```

---

## Key invariants (read these before changing anything)

### Stripe locks vs index locks

`Collection.stripe_locks` is a 1024-element array of `std.Io.Mutex` indexed by
`fnv1a(key) % 1024`. **Same-key writes serialize, different-key writes scale.**

But `hash_idx` and `key_doc_ids` and `ts_by_doc_id` are *single shared maps*.
Two writers in different stripes can both call `hash_idx.put()`. The stripe
lock is not enough.

→ `idx_mu` (a `std.Io.Mutex`) is the second lock. Every read/write of
`hash_idx`, `key_doc_ids`, or `ts_by_doc_id` must hold `idx_mu`.

This is the bug that crashed every concurrent-insert test before the fix.
**If you add a new shared collection-level map, it goes under `idx_mu`.**

### Stripe lock ordering for atomic txn

`applyTxn` takes multiple stripe locks. They must be acquired in **ascending
stripe-index order** to prevent deadlock with concurrent txns. The
implementation:

1. Walk ops, collect unique stripe indexes into a stack-allocated `[32]u32`
2. `std.mem.sort` ascending
3. Acquire each in order
4. Release in reverse via defer

If you add a "multi-collection txn", you'll need a second lock-ordering
dimension (collection name) above stripe index.

### Insert-then-validate is wrong; validate-then-insert is right

`applyTxn` does **two passes**:

1. Hold `idx_mu`, scan all ops, fail-fast on first invalid
2. Drop `idx_mu`, apply each op (which re-takes `idx_mu` per write)

This is the only way to get atomicity without an undo log. Don't try to
"validate as you go" — it lets a partial state escape.

### `_id` is the document key

`POST /db/:col` body with `"_id": "..."` makes the value of `_id` the
document key. Otherwise turbodb generates `doc_<ms>_<seq>`. This was
explicitly **not** the case for most of turbodb's history; legacy callers
that POST without `_id` still work via the auto-gen path.

`insertUnique` returns `error.AlreadyExists` on duplicate; the HTTP layer
converts to 409. `insert` (no Unique) silently overwrites — kept for the
auto-gen path where collisions are theoretically impossible (atomic seq).

### Engine timestamp

`nextEngineTs()` is a process-wide `std.atomic.Value(u64)` that:

- Ticks at least once per insert/update/delete
- Re-seeds from `compat.nanoTimestamp()` if wall clock moves forward
  (so values approximate real time and survive process restart by virtue of
  the wall clock — not durably, just monotonically)
- Stored per-doc in `Collection.ts_by_doc_id` (under `idx_mu`)
- Surfaced in HTTP responses as `"ts":N`

Don't replace `doc_id` with `ts` — `doc_id` is per-collection-monotonic and
also load-bearing as the MVCC anchor.

### Schema is opt-in and lives in a regular collection

`PUT /db/:col/_schema` writes the schema to a `_schemas` meta-collection
keyed by `<tenant>:<col>`. Validation runs in `validateSchema()` for every
insert/update. Collections without a schema doc skip validation entirely.

The validator is **token-shape only** — it checks the leading byte of each
field's JSON token (e.g. `c0 == '"'` for `str`, digit-or-`-` for `i64`,
`{` for `obj`). It does *not* parse numbers or run regex. This is fast and
catches the obvious mistakes; if you need full validation, layer JSON Schema
on top.

`u64` specifically rejects negative values; that was the
`debit_account_id=-5` case from the divergence probe.

---

## How to do common things

### Add a new HTTP endpoint

1. Find the slash-or-no-slash branch in `dispatch()` (`src/server.zig:~370`).
2. Match on `method` + `key` (when there's a `/db/:col/<x>` shape) or just
   `method` (when there's a `/db/:col` shape).
3. Write the handler. If it mutates: call `srv.db.recordTenantOperation()`
   first, `srv.recordQueryCost()` after. If it returns JSON: use the
   `getBodyBuf()` + `std.Io.Writer.fixed` pattern; cap at `MAX_BODY`.

### Add a new collection-level field

1. Declare it in `pub const Collection = struct { ... }` (`src/collection.zig`).
2. Initialize it in `Collection.init()` and clean up in `deinit()`.
3. If it's a shared map: protect with `idx_mu`, not stripe locks.
4. If you need to surface it in HTTP responses, wire it into both
   `handleGet` and the `handleScan` per-doc print loop. They have separate
   format strings — keep them in sync.

### Add a new query-string operator (`?foo=...`)

1. Add a field to `qe.Spec` in `src/query_engine.zig`.
2. Parse it in `Spec.parse()`.
3. Apply it in `handleScan`'s pipeline: filter → sort → limit → emit.
4. Add a unit test in `query_engine.zig` (it has its own `test` blocks).

### Add a new txn op kind

1. Add to `Collection.TxnOpKind` enum.
2. Add a case to the validate loop in `applyTxn` (precondition).
3. Add a case to the apply loop (write path — call the appropriate
   `*Locked` helper).
4. Add the JSON parser case in `handleTxn` in `src/server.zig`.
5. Document the precondition + behaviour in the comment block above
   `applyTxn`.

### Cross-compile + container

```bash
zig build -Dtarget=aarch64-linux-musl
container build --tag turbodb:test --file Containerfile --arch arm64 --os linux .
container delete --force tdb 2>/dev/null
container run --detach --name tdb --memory 2g \
  --publish 27017:27017 --publish 27018:27018 turbodb:test
```

If the container responds in `container exec` but not from `localhost:27018`,
the Apple container port-forward proxy is stuck. Fix:

```bash
container delete --force tdb
container system stop && container system start
container run --detach --name tdb ...
```

### Run the parity / divergence tests

```bash
# Start TigerBeetle
rm -f /tmp/tb_data.dat
cd /tmp/tigerbeetle
./zig-out/bin/tigerbeetle format --cluster=0 --replica=0 --replica-count=1 \
    --development /tmp/tb_data.dat
nohup ./zig-out/bin/tigerbeetle start --addresses=3001 --development \
    /tmp/tb_data.dat > /tmp/tb_server.log 2>&1 &

# Run probes
/Users/rachpradhan/.uv_env/base/bin/python3 parity/parity_test.py
/Users/rachpradhan/.uv_env/base/bin/python3 parity/divergence.py
```

### Stress

```bash
WORKERS=16 ITERS=100 bash stress/stress.sh         # mixed workload
DURATION=10 WORKERS=1 BATCH=8000 bash stress/bulk_throughput.sh   # peak throughput
```

---

## Numbers worth remembering

(Apple Silicon, debug build unless noted)

| Metric | Value |
|---|---|
| `zig build test` (core unit tests) | passes |
| Concurrent inserts (8 workers × 100, fixed) | 0 panics |
| Bulk insert, 1 worker × batch=8000 | 53,000 inserts/s |
| TigerBeetle bench (same Mac) | 28,260 tx/s |
| Container WAL recovery | 5/5 docs preserved across kill+restart |
| In-process INSERT (post-lock) | 348,160 ops/s |

---

## Open follow-ups (issues filed)

- **#127** crash-atomic txn WAL markers — `applyTxn` is in-memory atomic but
  not crash-atomic. Add txn_begin / txn_commit WAL markers + replay filter.

---

## Things this file deliberately doesn't cover

- The Calvin replication path (`src/replication/`). It works, has its own
  E2E test (`zig build test-calvin`), and is mostly orthogonal to the
  document-API work.
- The vector / trigram / columnar subsystems. They have their own bench
  files (`src/bench_*.zig`) and don't interact with the txn / schema /
  query-engine work documented here.
- The standalone test files for orphaned modules (parallel_wal.zig,
  marketplace.zig, etc.). Some of these are mid-Zig-0.16-migration and
  fail to compile in `zig build test-all`. They are not in the production
  binary and don't affect the running server.
