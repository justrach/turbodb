# TurboDB Roadmap — Engine-Native Cloud Features

The key advantage: **we own the engine**. Neon had to hack Postgres apart to separate compute and storage. Supabase runs vanilla Postgres and can only orchestrate around it. TurboDB is built from scratch — the storage format, wire protocol, query engine, replication, and compression are all ours.

Here's what that unlocks.

---

## 1. Instant Branching (Git for Data)

Copy-on-write forks at the storage layer. A branch is just a pointer + page diff — near-zero cost.

```
Production DB (customer-a)
    |
    +-- branch: staging    (CoW fork, near-zero cost)
    +-- branch: feature-x  (CoW fork)
    +-- branch: debug-jan5 (point-in-time snapshot)
```

Neon has this but it took them ages to bolt onto Postgres. TurboDB can build it natively — the mmap storage layer already knows what pages changed. Developers would kill for this.

**Implementation**: Extend mmap.zig to support CoW page tables. A fork shares all existing pages; writes go to a private overlay. Merge = replay the overlay onto the parent.

**Status**: Not started. Depends on: mmap page-level tracking.

---

## 2. Built-in Edge Replication

We control the replication protocol (Calvin). Instead of Postgres's heavy streaming replication:

```
Primary (US-East)
    | lightweight sync
    +-- Read replica (EU) — 50ms away
    +-- Read replica (Asia) — 100ms away
    +-- Embedded replica (user's app, local)
```

That last one is the Turso/libSQL play — an **embedded read replica** that ships inside the customer's app. Reads are local (0ms), writes go to primary. Because we own the wire format, we can make this tiny and efficient.

**Implementation**: Calvin already batches + serializes transactions (419 bytes for 5 txns). Ship the batch stream to edge replicas. For embedded mode, compile TurboDB as a library (libturbodb.dylib — already exists) and add a sync client.

**Status**: Calvin replication working (5/5 consistency verified in Docker cluster). Edge sync protocol not started.

---

## 3. Query-Aware Scale-to-Zero

Generic platforms can only watch "is there a TCP connection?" We go deeper:

```
DEEP SLEEP:  no connections for 30min -> snapshot to S3, free all RAM
LIGHT SLEEP: connected but idle queries -> shrink buffer pool to 4MB
WARM:        active queries -> full resources
HOT:         heavy load -> auto-expand buffers, spawn read replicas
```

We can instrument **inside the query engine** to know the difference between "connected but idle" and "actually working." No external platform can do this.

**Implementation**: Add query activity tracking to wire.zig (last_query_time, queries_per_second). The cloud control plane reads these metrics and adjusts resource allocation. Deep sleep = flush WAL, snapshot pages to object storage, terminate process. Wake = restore from snapshot.

**Status**: Wire server tracks connections. Query-level metrics not yet exposed.

---

## 4. Native Multi-Tenant Mode

Instead of one-process-per-customer, TurboDB itself handles isolation:

```
Single turbodb process
    +-- tenant: customer-a (isolated keyspace, own auth)
    +-- tenant: customer-b (isolated keyspace, own auth)
    +-- tenant: customer-c (isolated keyspace, own auth)
```

One process, shared buffer pool, but **hard isolation at the engine level** — not row-level security bolted on top. Separate keyspaces, separate auth, separate resource quotas. This is what CockroachDB and Vitess do, but native from day one.

Economics:
- 1000 tenants in one process vs 1000 separate processes
- Shared memory, shared CPU, isolated data
- 10-100x better resource utilization

**Implementation**: Extend Database to support namespaced collections (tenant_id/collection_name). Auth module already has per-key permissions — add tenant_id to KeyEntry. Add per-tenant resource quotas (max collections, max storage, max ops/sec).

**Status**: Auth module exists (src/auth.zig). Multi-tenant namespacing not started.

---

## 5. Time Travel Queries

If TurboDB keeps versioned pages or a WAL, every query can look at any point in time:

```
GET /db/orders/ord-001?as_of=2026-03-25T14:00:00Z
```

Built into the engine, not a plugin. Every customer gets automatic point-in-time queries. Debugging production issues becomes trivial — "what did this row look like yesterday?"

**Implementation**: MVCC version chains (src/mvcc.zig) already track multiple versions per document with epoch numbers. Map wall-clock timestamps to epochs. Add `as_of` parameter to GET/scan operations that reads the version chain at the specified epoch instead of HEAD.

**Status**: MVCC working (41.8M read txn/s). Timestamp-to-epoch mapping not yet built.

---

## 6. Programmable Webhooks / Engine-Level CDC

Not Postgres-style triggers (slow, run in-process). Native change data capture:

```
ON INSERT INTO orders -> HTTP POST to customer's webhook
ON CHANGE IN users WHERE role = 'admin' -> push event to stream
```

Because we own the WAL, we can stream changes out natively — like a built-in CDC. Customers get real-time events without polling. This is what Supabase Realtime does, but they had to build a whole Elixir service to tail Postgres WAL. We'd have it **inside the engine**.

**Implementation**: WAL already has an EntryIterator. Add a WAL tailer that runs in a background thread, filters entries against registered subscriptions, and fires HTTP webhooks or pushes to a WebSocket stream. Use the auth module's HMAC-SHA256 for webhook signatures.

**Status**: WAL infrastructure exists (src/storage/wal.zig). Subscription API and webhook dispatch not started. HMAC-SHA256 available in src/crypto.zig.

---

## 7. Per-Query Cost Metering

We own the query executor, so we can track everything:

```json
{
  "tenant": "customer-a",
  "query": "scan users limit=100",
  "rows_scanned": 14000,
  "bytes_read": 2100000,
  "cpu_us": 12,
  "cost_usd": 0.000003
}
```

True usage-based pricing at the query level. Not "you used X GB of storage and Y hours of compute" — actual **per-query billing** like BigQuery. No one in the embedded/OLTP database space does this well.

**Implementation**: Wrap collection operations with instrumentation counters (rows scanned, bytes read, time elapsed). Emit per-query metrics to a billing log. Cloud control plane aggregates and bills via Stripe metering API.

**Status**: Basic request metrics exist in server.zig (req_count, err_count). Per-query instrumentation not started.

---

## 8. Snapshot Sharing / Dataset Marketplace

Since we control the snapshot format:

```
Customer publishes: "US Census 2025 dataset"
    -> stored as TurboDB snapshot on S3
    -> another customer: "fork this dataset"
    -> instant CoW clone into their account
```

A dataset marketplace where provisioning is instant because it's just forking a snapshot. Zero data copying.

**Implementation**: Depends on CoW branching (feature 1). A "published snapshot" is a read-only CoW base. Forking = creating a new overlay on top of it. Storage backend needs S3/R2 support for snapshot persistence.

**Status**: Not started. Depends on: CoW branching, object storage backend.

---

## Priority Matrix

| Feature | Impact | Effort | Dependencies |
|---------|--------|--------|-------------|
| **Multi-tenancy** | Critical for cloud economics | Medium | Auth module (done) |
| **Time travel queries** | High differentiator | Easy | MVCC (done) |
| **CDC / webhooks** | Table stakes for cloud DB | Medium | WAL (done), crypto (done) |
| **Per-query metering** | Required for billing | Easy | Server metrics (partial) |
| **Scale-to-zero** | Cost efficiency | Medium | Query metrics |
| **CoW branching** | Killer feature | Hard | mmap page tracking |
| **Edge replication** | Competitive moat | Hard | Calvin (done) |
| **Snapshot marketplace** | Long-term play | Hard | CoW branching, S3 |

**Recommended order**: Multi-tenancy → Time travel → CDC/webhooks → Per-query metering → Scale-to-zero → CoW branching → Edge replication → Marketplace

---

## The Moat

```
+-- TurboDB Cloud -----------------------------------------------+
|                                                                 |
|  dashboard (control plane)                                      |
|  +-- Provision instances                                        |
|  +-- Branch / fork / time-travel                                |
|  +-- Per-query billing dashboard                                |
|  +-- Dataset marketplace                                        |
|                                                                 |
|  turbodb (the engine WE own)                                    |
|  +-- Native multi-tenancy                                       |
|  +-- Built-in CDC / webhooks                                    |
|  +-- Embedded read replicas                                     |
|  +-- Query-aware sleep states                                   |
|  +-- CoW branching at storage layer                             |
|                                                                 |
|  Runs on: one Hetzner box to start                              |
|  Scales to: cluster of boxes with Calvin + placement layer      |
+-----------------------------------------------------------------+
```

The moat is **vertical integration**. Supabase can never do half of this because they don't own Postgres. Neon can do some of it but they're constrained by Postgres's architecture. PlanetScale owns Vitess but it's MySQL-flavored and complex.

TurboDB is a clean-slate database built from scratch. That's rare. That's the advantage.
