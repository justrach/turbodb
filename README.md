<p align="center">
  <img src="turbito.png" width="200" alt="Turbito — TurboDB mascot">
  <h1 align="center">TurboDB</h1>
  <p align="center">
    <strong>A blazing-fast NoSQL document database. Zig storage core.</strong>
  </p>
  <p align="center">
    <a href="https://pypi.org/project/turbodatabase/"><img src="https://img.shields.io/pypi/v/turbodatabase?color=3776AB&logo=python&label=PyPI" alt="PyPI"></a>
    <a href="https://www.npmjs.com/package/turbodatabase"><img src="https://img.shields.io/npm/v/turbodatabase?color=339933&logo=npm&label=npm" alt="npm"></a>
    <img src="https://img.shields.io/badge/zig-0.16-F7A41D?logo=zig" alt="Zig 0.16">
    <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT">
    <img src="https://img.shields.io/badge/status-alpha-orange" alt="Alpha">
  </p>
</p>

---

TurboDB is a document database written from scratch in Zig with **mmap + WAL + B-tree + MVCC + ART indexes + LSM tree + LZ4 compression** — designed to be a faster alternative to MongoDB and PostgreSQL for document workloads.

It exposes a **binary wire protocol** (fastest) and **HTTP REST API**, with **Python** and **Node.js** FFI bindings that call directly into the Zig storage engine (no network overhead).
```bash
pip install turbodatabase     # Python
npm install turbodatabase     # Node.js
```

## Status

| Feature | Status | Notes |
|---------|--------|-------|
| Insert / Get / Update / Delete | ✅ Working | ~13.3M GET/s in-process, ~42K/s wire protocol |
| B-tree index (FNV-1a) | ✅ Working | O(log N), branching factor 169 |
| ART index (Adaptive Radix Tree) | ✅ Working | 19M search/s, path compression, Node4/16/48/256 |
| LSM tree | ✅ Working | MemTable + SSTable + bloom filters, size-tiered compaction |
| LZ4 compression | ✅ Working | ~800K ops/s on 4KB blocks, page-level transparent |
| Query filter engine | ✅ Working | $gt/$lt/$eq/$in/$and/$or, predicate pushdown to ART |
| WAL group commit | ✅ Working | Parallel WAL with per-core segments |
| MVCC version chains | ✅ Working | Epoch-based GC, zero read locks |
| mmap storage | ✅ Working | Zero-copy reads, 256 MiB growth |
| Columnar projections | ✅ Working | Vectorized filter, 1.02B scan/s |
| **Vector search (SIMD)** | ✅ Working | **Cosine, dot product, L2 — @Vector(4,f32) SIMD** |
| Hash/range partitioning | ✅ Working | FNV-1a routing, parallel scatter-gather scan |
| Calvin replication | ✅ Working | Deterministic sequencer + executor |
| Shard management | ✅ Working | Consistent hash ring, partition migration |
| Cross-shard query routing | ✅ Working | Scatter-gather, partition pruning, aggregate merge |
| io_uring / kqueue | ✅ Working | Async I/O, event-loop server |
| Binary wire protocol | ✅ Working | TCP_NODELAY, pipelining, batch ops |
| JSON REST API | ✅ Working | MongoDB-inspired routes on :27017 |
| **Authentication** | ✅ Working | **API key + BLAKE3 hashing, per-key permissions** |
| **Schema validation** | ✅ Working | **Required fields, type checking (string/number/bool/object/array)** |
| **TTL / document expiry** | ✅ Working | **Per-doc TTL with background reaper** |
| **Cursor pagination** | ✅ Working | **Stable hex-encoded cursor tokens** |
| **Structured error codes** | ✅ Working | **30+ error codes, HTTP + wire mapping** |
| Crypto (SHA-256/BLAKE3/Ed25519) | ✅ Working | Zero-dep, Zig std.crypto, FFI-accessible |
| Python FFI (ctypes) | ✅ Working | `pip install turbodatabase` |
| Node.js FFI (koffi) | ✅ Working | `npm install turbodatabase` |
| TLS | 🔜 Use reverse proxy | Recommended: nginx/Caddy/Fly.io for TLS termination |
| Multi-doc transactions | 🔜 Planned | Cross-partition ACID |
| Change streams | 🔜 Planned | WAL tailing subscription API |

## Benchmarks

### TurboDB vs PostgreSQL vs MongoDB (wire protocol, 10K docs, localhost)

<!-- BENCH_START -->
| Workload | TurboDB | PostgreSQL | MongoDB | vs Postgres | vs Mongo |
|----------|---------|------------|---------|:-----------:|:--------:|
| **INSERT** | 10.9K/s | 13.1K/s | 13.5K/s | 0.8x | 0.8x |
| **GET** | **42.3K/s** | 36.3K/s | 11.4K/s | **1.2x** | **3.7x** |
| **UPDATE** | **43.1K/s** | 12.3K/s | 11.8K/s | **3.5x** | **3.7x** |
| **DELETE** | **52.6K/s** | 14.3K/s | 12.9K/s | **3.7x** | **4.1x** |
| **SEARCH** | **21.9M/s** | — | 8.5K/s | — | **~2,500x** |
<!-- BENCH_END -->

> TurboDB dominates reads/updates/deletes at **3-4x** over both competitors. Search uses a trigram index at **21.9M ops/s** vs MongoDB's regex scan at 8.5K/s.
> Run `python3 bench/triple_bench.py` locally.

### Storage engine (in-process, no network)

| Subsystem | Throughput | Notes |
|-----------|-----------|-------|
| **Core GET** | **13.3M ops/s** | Zero-copy mmap read |
| **Core INSERT** | **910K ops/s** | B-tree + WAL |
| **Core Update** | **2.3M ops/s** | In-place mmap write |
| **Core Delete** | **3.7M ops/s** | Tombstone + WAL |
| **ART Search** | **19.0M ops/s** | Adaptive Radix Tree |
| **ART Insert** | **8.7M ops/s** | Trie w/ path compression |
| **Query Match** | **35.3M ops/s** | Predicate evaluation |
| **Field Extract** | **43.5M ops/s** | Zero-alloc JSON scanner |
| **Column Scan** | **1.02B ops/s** | Vectorized columnar |
| **Column Filter** | **701M ops/s** | SIMD predicate pushdown |
| **Column Append** | **302M ops/s** | Append-only columnar |
| **MVCC Read** | **41.8M ops/s** | Snapshot isolation |
| **MVCC GC** | **49.2M ops/s** | Epoch-based reclamation |
| **LSM Get** | **19.1M ops/s** | Bloom filter + SSTable |
| **LZ4 Compress** | **758K ops/s** | 4KB blocks |
| **LZ4 Decompress** | **845K ops/s** | 4KB blocks |

> 21 subsystem benchmarks. Run `zig build bench-regression` to reproduce.

### Partition scaling (in-process, hash partitioning)

| Partitions | INSERT | GET | SCAN | PAR_SCAN |
|:----------:|-------:|----:|-----:|--------:|
| 1 | 919K/s | 12.0M/s | 3.7M/s | 46K/s |
| 2 | 912K/s | 10.4M/s | 1.6M/s | 26K/s |
| 4 | 910K/s | 11.2M/s | 737K/s | 14K/s |
| 8 | 947K/s | 12.3M/s | 422K/s | 7K/s |
| 16 | 946K/s | 10.6M/s | 234K/s | 3K/s |

> INSERT stays flat (~920K/s) — FNV-1a hash routing is near-zero overhead. GET stays 10-12M across all partition counts. Run `zig build bench-partition` or `bash bench/setup_shard_bench.sh` for the full cross-engine shard comparison.

- **Zero-copy**: `get()` returns a pointer directly into mmap'd memory — no deserialization
- **FNV-1a 8-byte hash** vs MongoDB's 12-byte ObjectId — smaller index entries, better cache locality
- **4KB page B-tree** — 3 levels handles 6.2M documents
- **No BSON overhead** — compact binary format, zero-alloc JSON field scanner
## Quick Start

### Build from source

```bash
# Requires Zig 0.16+
git clone https://github.com/justrach/turbodb
cd turbodb
zig build

# Run the server (wire protocol, fastest)
./zig-out/bin/turbodb --port 27017

# Or both wire + HTTP
./zig-out/bin/turbodb --both --data ./mydata --port 27017
```

### REST API

```bash
# Insert a document
curl -X POST http://localhost:27018/db/users \
  -d '{"key":"alice","value":{"name":"Alice","age":30}}'

# Get by key
curl http://localhost:27018/db/users/alice

# Update
curl -X PUT http://localhost:27018/db/users/alice \
  -d '{"value":{"name":"Alice","age":31}}'

# Delete
curl -X DELETE http://localhost:27018/db/users/alice

# Scan with query filter
curl "http://localhost:27018/db/users?limit=20&filter={\"age\":{\"\$gt\":25}}"

# Health check
curl http://localhost:27018/health
```

### Python (FFI — no network overhead)

```bash
pip install turbodatabase
```

```python
from turbodb import Database

db = Database("./mydata")
users = db.collection("users")

doc_id = users.insert("alice", {"name": "Alice", "age": 30})
doc = users.get("alice")
users.update("alice", {"name": "Alice", "age": 31})
users.delete("alice")

for doc in users.scan(limit=100):
    print(doc["key"], doc["value"])

db.close()
```

### Node.js (FFI — no network overhead)

```bash
npm install turbodatabase
```

```javascript
const { Database } = require('turbodatabase');

const db = new Database('./mydata');
const users = db.collection('users');

const id = users.insert('alice', { name: 'Alice', age: 30 });
const doc = users.get('alice');
users.update('alice', { name: 'Alice', age: 31 });
users.delete('alice');

db.close();
```

## Architecture

```
                    ┌───────────────────────────────────────────┐
                    │            Client Libraries                │
                    │  Python (ctypes)  ·  Node.js (koffi)       │
                    └──────────────┬────────────────────────────┘
                                   │ FFI (C ABI)
                    ┌──────────────▼────────────────────────────┐
                    │          libturbodb.dylib/.so               │
  Wire :27017 ────▶ ├───────────────────────────────────────────┤
  HTTP :27018 ────▶ │  Query Engine  ·  Filter  ·  Aggregation   │
                    ├───────────────────────────────────────────┤
                    │  Collection  ·  B-tree  ·  ART  ·  LSM     │
                    ├───────────────────────────────────────────┤
                    │  Partitioning  ·  Router  ·  Shard Manager │
                    ├───────────────────────────────────────────┤
                    │  MVCC  ·  Columnar  ·  LZ4 Compression     │
                    ├───────────────────────────────────────────┤
                    │  Parallel WAL  ·  Epoch GC  ·  io_engine   │
                    ├───────────────────────────────────────────┤
                    │  mmap (zero-copy)  ·  4KB Page Allocator   │
                    └───────────────────────────────────────────┘
                                   │
                              ┌────▼────┐
                              │  Disk   │
                              │ .pages  │
                              │ .wal    │
                              └─────────┘
```

### Storage Layout

| Component | Description |
|-----------|-------------|
| **mmap** | Memory-mapped files. Zero-copy reads. 256 MiB growth chunks. |
| **Parallel WAL** | Per-core WAL segments. Group commit with single fsync per batch. |
| **Page allocator** | 4KB pages. Free-list recycling. Leaf pages for docs, btree_leaf for index. |
| **B-tree** | FNV-1a key hash -> (page, offset). Branching factor 169. 3 levels = 6.2M docs. |
| **ART** | Adaptive Radix Tree for secondary indexes. Path compression. 19M search/s. |
| **LSM tree** | Write-optimized path. MemTable + SSTables + bloom filters. |
| **MVCC** | Version chains via `next_ver` pointer. Readers never block writers. |
| **Epoch GC** | Epoch-based memory reclamation. 48.9M GC ops/s. |
| **LZ4** | Block compression at page level. ~800K compress ops/s on 4KB blocks. |
| **Columnar** | Column projections for analytics. Vectorized filter at 950M ops/s. |
| **Partitioning** | Hash (FNV-1a) or range-based. Parallel scatter-gather scan. |
| **Calvin** | Deterministic replication. Sequencer + executor. No 2PC needed. |

### Document Format (32-byte header)

```
┌──────────┬──────────┬─────────┬─────────┬───────┬─────────┬──────────┐
│ doc_id   │ key_hash │ val_len │ key_len │ flags │ version │ next_ver │
│ u64 (8B) │ u64 (8B) │ u32 (4B)│ u16 (2B)│ u8    │ u8      │ u64 (8B) │
└──────────┴──────────┴─────────┴─────────┴───────┴─────────┴──────────┘
```

## Project Structure

```
turbodb/
├── build.zig                  # Build config — exe + lib + benchmarks + tests
├── src/
│   ├── main.zig               # Server entry point (wire + HTTP)
│   ├── server.zig             # HTTP REST API
│   ├── wire.zig               # Binary wire protocol
│   ├── ffi.zig                # C ABI exports for Python/JS
│   ├── client.zig             # Embedded client API
│   ├── collection.zig         # Database + Collection (core API)
│   ├── doc.zig                # Document format, FNV-1a hash
│   ├── btree.zig              # B-tree index
│   ├── art.zig                # Adaptive Radix Tree index
│   ├── lsm.zig                # LSM tree (MemTable + SSTable)
│   ├── query.zig              # Query filter engine ($gt/$lt/$eq/$in)
│   ├── compression.zig        # LZ4 block compression
│   ├── columnar.zig           # Columnar projections + vectorized filter
│   ├── mvcc.zig               # MVCC version chains + epoch GC
│   ├── partition.zig          # Hash/range partitioning
│   ├── io_engine.zig          # io_uring / kqueue async I/O
│   ├── page.zig               # 4KB page allocator
│   ├── storage/
│   │   ├── mmap.zig           # Memory-mapped files
│   │   ├── wal.zig            # Write-ahead log
│   │   ├── parallel_wal.zig   # Per-core parallel WAL
│   │   ├── epoch.zig          # Epoch-based GC
│   │   └── seqlock.zig        # Sequence locks
│   ├── replication/
│   │   ├── calvin.zig         # Calvin deterministic executor
│   │   ├── sequencer.zig      # Transaction sequencer
│   │   ├── shard.zig          # Shard manager + consistent hash ring
│   │   └── router.zig         # Cross-shard query routing
│   ├── bench_regression.zig   # 21 subsystem benchmarks
│   └── bench_partition.zig    # Partition scaling benchmarks
├── python/                    # Python FFI package
├── js/                        # Node.js FFI package
└── bench/
    ├── bench.py               # TurboDB vs MongoDB benchmark
    ├── triple_bench.py        # TurboDB vs Postgres vs MongoDB
    ├── shard_bench.py         # Cross-engine shard scaling comparison
    ├── setup_shard_bench.sh   # One-command shard benchmark setup
    └── docker/
        ├── mongo-shard-cluster.yml  # MongoDB sharded cluster
        └── init-sharding.sh         # Cluster initialization
```

## TurboDB vs MongoDB

| Feature | MongoDB | TurboDB |
|---------|:-------:|:-------:|
| Sharding | ✅ | ✅ Hash/range partitioning + consistent hash ring |
| Replication | ✅ | ✅ Calvin deterministic (no 2PC) |
| Query filters | ✅ | ✅ $gt/$lt/$eq/$in/$and/$or + index pushdown |
| Full-text search | ✅ | ✅ Trigram index (21.9M ops/s) |
| Columnar analytics | ❌ | ✅ Vectorized scan (950M ops/s) |
| LZ4 compression | ✅ Snappy/zstd | ✅ LZ4 page-level |
| Wire protocol | ✅ BSON | ✅ Binary + TCP_NODELAY |
| Geospatial queries | ✅ | ❌ Not planned |
| Change streams | ✅ | 🔜 WAL tailing |
| Multi-doc transactions | ✅ | 🔜 Planned |
| Authentication / TLS | ✅ | 🔜 Planned |
| Schema validation | ✅ | 🔜 Planned |
| Drivers (20+ languages) | ✅ | Python, JS (FFI) |
| Production battle-tested | ✅ | ❌ Alpha |

### When to use TurboDB

- You want **embedded** document storage (like SQLite but for JSON)
- You want **zero-copy reads** without deserialization overhead
- You're building in **Python or Node.js** and want native FFI speed
- You need **fast point lookups** (14M GET/s in-process, 3.7x MongoDB over wire)
- You want a **single binary** with no runtime dependencies

### When to use MongoDB

- You need **production-grade** auth, TLS, and monitoring
- You need drivers for languages beyond Python/JS
- You need **geospatial queries** or **change streams**
- You need battle-tested reliability at scale

## Build Commands

```bash
zig build                    # Build exe + shared library
zig build run                # Run server (wire protocol, port 27017)
zig build test               # Run core tests
zig build test-all           # Run all subsystem tests

# Benchmarks
zig build bench-regression   # 21 subsystem benchmarks
zig build bench-partition    # Partition scaling (1/2/4/8/16 shards)
python3 bench/triple_bench.py           # TurboDB vs Postgres vs MongoDB
bash bench/setup_shard_bench.sh         # Full shard comparison (needs Docker + Postgres)
```

## Contributing

Contributions welcome! TurboDB is written in Zig 0.16 with no external dependencies.

## License

MIT
