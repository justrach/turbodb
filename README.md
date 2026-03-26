<p align="center">
  <img src="turbito.png" width="200" alt="Turbito — TurboDB mascot">
  <h1 align="center">TurboDB</h1>
  <p align="center">
    <strong>A blazing-fast NoSQL document database. Zig storage core.</strong>
  </p>
  <p align="center">
    <a href="https://pypi.org/project/turbodatabase/"><img src="https://img.shields.io/pypi/v/turbodatabase?color=3776AB&logo=python&label=PyPI" alt="PyPI"></a>
    <a href="https://www.npmjs.com/package/turbodatabase"><img src="https://img.shields.io/npm/v/turbodatabase?color=339933&logo=npm&label=npm" alt="npm"></a>
    <img src="https://img.shields.io/badge/zig-0.15-F7A41D?logo=zig" alt="Zig 0.15">
    <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT">
    <img src="https://img.shields.io/badge/status-alpha-orange" alt="Alpha">
  </p>
</p>

---

TurboDB is a document database written from scratch in Zig with **mmap + WAL + B-tree + MVCC** — designed to be a faster, simpler alternative to MongoDB for single-node deployments.

It exposes a **MongoDB-compatible REST API** and ships with **Python** and **Node.js** FFI bindings that call directly into the Zig storage engine (no HTTP overhead).

```bash
pip install turbodatabase     # Python
npm install turbodatabase     # Node.js
```

## Status

| Feature | Status | Notes |
|---------|--------|-------|
| Insert / Get / Update / Delete | ✅ Working | ~15K ops/s single-thread HTTP |
| B-tree index (FNV-1a) | ✅ Working | O(log N), branching factor 169 |
| WAL group commit | ✅ Working | Single fsync per batch |
| MVCC version chains | ✅ Working | Zero read locks |
| mmap storage | ✅ Working | Zero-copy reads, 256 MiB growth |
| JSON REST API | ✅ Working | MongoDB-inspired routes on :27017 |
| Python FFI (ctypes) | ✅ Working | `pip install turbodatabase` |
| Node.js FFI (koffi) | ✅ Working | `npm install turbodatabase` |
| Collection scan | ✅ Working | Limit/offset pagination |
| Sharding | 🔜 Planned | Consistent hash ring |
| Replication | 🔜 Planned | WAL-based leader-follower |
| Aggregation pipeline | 🔜 Planned | Map-reduce style |
| Full-text search | 🔜 Planned | Inverted index |
| TTL / expiry | 🔜 Planned | Background compaction |
| Authentication | 🔜 Planned | Token-based |
| TLS | 🔜 Planned | Native Zig TLS |
| Transactions | 🔜 Planned | Multi-doc ACID |

## Benchmarks

Single connection, 256B documents, Apple M3, Zig 0.15 (Debug build):

| Operation | TurboDB | MongoDB 8.2 (ref) | Speedup |
|-----------|---------|-------------------|---------| 
| **Insert** | 14,379 ops/s | ~18,000 ops/s | 0.8x |
| **Get** | 15,099 ops/s | ~12,000 ops/s | **1.3x** |
| **Update** | 18,105 ops/s | ~16,000 ops/s | **1.1x** |
| **Delete** | 16,407 ops/s | ~20,000 ops/s | 0.8x |

> **Note**: TurboDB is compiled in Debug mode. Release builds are 3-5x faster.
> MongoDB reference numbers are single-node localhost with default config.
> Run `python3 bench/bench.py` for a full head-to-head comparison ([idealo/mongodb-benchmarking](https://github.com/idealo/mongodb-benchmarking) style).

### Why faster reads?

- **Zero-copy**: `get()` returns a pointer directly into mmap'd memory — no deserialization
- **FNV-1a 8-byte hash** vs MongoDB's 12-byte ObjectId — smaller index entries, better cache locality
- **4KB page B-tree** — 3 levels handles 6.2M documents
- **No BSON overhead** — compact binary format, zero-alloc JSON field scanner

## Quick Start

### Build from source

```bash
# Requires Zig 0.15+
git clone https://github.com/justrach/turbodb
cd turbodb
zig build

# Run the server
./zig-out/bin/turbodb --port 27017

# Or with custom data directory
./zig-out/bin/turbodb --data ./mydata --port 27017
```

### REST API

```bash
# Insert a document
curl -X POST http://localhost:27017/db/users \
  -d '{"key":"alice","value":{"name":"Alice","age":30}}'

# Get by key
curl http://localhost:27017/db/users/alice

# Update
curl -X PUT http://localhost:27017/db/users/alice \
  -d '{"value":{"name":"Alice","age":31}}'

# Delete
curl -X DELETE http://localhost:27017/db/users/alice

# Scan collection
curl "http://localhost:27017/db/users?limit=20&offset=0"

# List collections
curl http://localhost:27017/collections

# Health check
curl http://localhost:27017/health
```

### Python (FFI — no HTTP overhead)

```bash
pip install turbodatabase
```

```python
from turbodb import Database

# Opens/creates database directory
db = Database("./mydata")
users = db.collection("users")

# Insert (value can be dict or string)
doc_id = users.insert("alice", {"name": "Alice", "age": 30})

# Get
doc = users.get("alice")
print(doc)  # {'key': 'alice', 'value': '{"name":"Alice","age":30}', 'doc_id': 1, 'version': 0}

# Update
users.update("alice", {"name": "Alice", "age": 31})

# Delete
users.delete("alice")

# Scan
for doc in users.scan(limit=100):
    print(doc["key"], doc["value"])

# Context manager
with Database("./mydata") as db:
    col = db.collection("events")
    col.insert("evt1", {"type": "click", "ts": 1234567890})

db.close()
```

### Node.js (FFI — no HTTP overhead)

```bash
npm install turbodatabase
```

```javascript
const { Database } = require('turbodatabase');

const db = new Database('./mydata');
const users = db.collection('users');

// Insert
const id = users.insert('alice', { name: 'Alice', age: 30 });

// Get
const doc = users.get('alice');
console.log(doc); // { key: 'alice', value: '{"name":"Alice","age":30}', doc_id: 1, version: 0 }

// Update, Delete, Scan
users.update('alice', { name: 'Alice', age: 31 });
users.delete('alice');
const docs = users.scan(100);

db.close();
```

## Architecture

```
                    ┌─────────────────────────────────────┐
                    │          Client Libraries            │
                    │  Python (ctypes)  ·  Node.js (koffi) │
                    └──────────────┬──────────────────────┘
                                   │ FFI (C ABI)
                    ┌──────────────▼──────────────────────┐
                    │         libturbodb.dylib/.so         │
  HTTP :27017 ────▶ ├─────────────────────────────────────┤
                    │  Collection  ·  B-tree  ·  MVCC      │
                    ├─────────────────────────────────────┤
                    │  4KB Page Allocator  ·  Free List     │
                    ├─────────────────────────────────────┤
                    │  WAL (group commit)  ·  Epoch GC      │
                    ├─────────────────────────────────────┤
                    │  mmap (zero-copy)  ·  256 MiB growth  │
                    └─────────────────────────────────────┘
                                   │
                              ┌────▼────┐
                              │  Disk   │
                              │ .tdb    │
                              │ .wal    │
                              └─────────┘
```

### Storage Layout

| Component | Description |
|-----------|-------------|
| **mmap** | Memory-mapped files. Zero-copy reads. 256 MiB growth chunks. |
| **WAL** | Write-ahead log. Group commit with single fsync per batch. Crash recovery. |
| **Page allocator** | 4KB pages. Free-list recycling. Leaf pages for docs, btree_leaf for index. |
| **B-tree** | FNV-1a key hash → (page, offset). Branching factor 169. 3 levels = 6.2M docs. |
| **MVCC** | Version chains via `next_ver` pointer. Readers never block writers. |
| **Epoch GC** | Epoch-based memory reclamation for safe concurrent access. |

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
├── build.zig              # Zig build — exe + shared library
├── build.zig.zon          # Package manifest
├── turbito.png            # Turbito — the TurboDB mascot
├── src/
│   ├── main.zig           # Server entry point
│   ├── server.zig         # HTTP REST API
│   ├── ffi.zig            # C ABI exports for Python/JS
│   ├── collection.zig     # Database + Collection (core API)
│   ├── doc.zig            # Document format, FNV-1a hash
│   ├── btree.zig          # B-tree index
│   ├── page.zig           # 4KB page allocator
│   └── storage/
│       ├── mmap.zig       # Memory-mapped files
│       ├── wal.zig        # Write-ahead log
│       ├── epoch.zig      # Epoch-based GC
│       └── seqlock.zig    # Sequence locks
├── python/
│   ├── turbodb/           # Python package (ctypes FFI)
│   │   ├── __init__.py    # Database, Collection classes
│   │   └── _ffi.py        # Low-level ctypes bindings
│   ├── test.py            # Smoke test
│   └── pyproject.toml
├── js/
│   ├── index.js           # Node.js package (koffi FFI)
│   ├── test.js            # Smoke test
│   └── package.json
└── bench/
    └── bench.py           # Benchmark (idealo/mongodb-benchmarking style)
```

## What's Missing (vs MongoDB)

TurboDB is a single-node embedded database. Here's what it **doesn't** have yet:

| Feature | MongoDB | TurboDB | Notes |
|---------|---------|---------|-------|
| Sharding | ✅ | ❌ | Planned: consistent hash ring |
| Replication | ✅ | ❌ | Planned: WAL-based |
| Aggregation | ✅ | ❌ | Planned: map-reduce |
| Full-text search | ✅ | ❌ | Planned: inverted index |
| Geospatial queries | ✅ | ❌ | Not planned |
| Change streams | ✅ | ❌ | Planned: WAL tailing |
| Transactions (multi-doc) | ✅ | ❌ | Planned |
| Authentication / RBAC | ✅ | ❌ | Planned |
| TLS | ✅ | ❌ | Planned |
| Schema validation | ✅ | ❌ | Planned: JSON Schema |
| Capped collections | ✅ | ❌ | Planned |
| GridFS | ✅ | ❌ | Not planned |
| Wire protocol (BSON) | ✅ | ❌ | Uses JSON REST instead |
| Drivers (20+ languages) | ✅ | Python, JS | FFI-based |
| Production battle-tested | ✅ | ❌ | Alpha |

### When to use TurboDB

- You want **embedded** document storage (like SQLite but for JSON)
- You want **zero-copy reads** without deserialization overhead
- You're building in **Python or Node.js** and want native FFI speed
- You need a **simple, fast** single-node store and don't need sharding
- You want to **learn** how document databases work internally

### When to use MongoDB

- You need **horizontal scaling** (sharding, replica sets)
- You need the **aggregation pipeline** or full-text search
- You need **production-grade** auth, TLS, and monitoring
- You need drivers for languages beyond Python/JS
- You need **multi-document transactions**

## Build Commands

```bash
zig build              # Build exe + shared library
zig build run          # Run server (port 27017)
zig build lib          # Build just libturbodb.dylib/.so

# Run benchmarks
python3 bench/bench.py --docs 10000 --threads 10

# Run tests
python3 python/test.py
cd js && npm install && node test.js
```

## Contributing

Contributions welcome! TurboDB is written in Zig 0.15 with no external dependencies.

## License

MIT
