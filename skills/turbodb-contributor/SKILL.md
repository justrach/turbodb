---
name: turbodb-contributor
description: Use when modifying turbodb engine, HTTP API, schema validation, transaction, WAL, parity, or stress-test behavior. Follow the repository invariants in AGENTS.md before changing Collection, Server, query, storage, or replication code.
---

# Turbodb Contributor

## First read

Read `AGENTS.md` before making behavior changes. It is the source of truth for
lock ordering, index ownership, transaction atomicity, schema behavior, engine
timestamps, and the expected validation commands.

## Core workflow

1. Inspect the touched subsystem before editing. Prefer `rg` and focused file
   reads over broad scans.
2. Preserve the invariants from `AGENTS.md`, especially:
   - `hash_idx`, `key_doc_ids`, and `ts_by_doc_id` require `idx_mu`.
   - Atomic transactions acquire unique stripe locks in ascending stripe index.
   - Transactions validate all operations before applying any operation.
   - `_id` is the document key for user-supplied IDs.
   - Engine timestamps are metadata; do not replace `doc_id` with `ts`.
   - Schemas are opt-in and stored in `_schemas`.
3. Keep HTTP response formats in sync between single-document reads and scans
   when adding per-document metadata.
4. For new mutation endpoints, record tenant operation cost before the write
   path and query cost after the handler work.
5. Keep tests proportional to risk. Query operators should get
   `src/query_engine.zig` tests; engine/API behavior should be validated with
   `zig build test` plus a targeted HTTP or parity probe when relevant.

## Common commands

```bash
zig build test
zig build
zig build -Dtarget=aarch64-linux-musl
WORKERS=16 ITERS=100 bash stress/stress.sh
/Users/rachpradhan/.uv_env/base/bin/python3 parity/divergence.py
```

## Before publishing

Run the most relevant build or test command that covers the changed behavior.
If container behavior changed, cross-compile and rebuild `Containerfile` before
reporting that the change is ready.
