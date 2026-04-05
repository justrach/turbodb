"""
TurboDB — Python client (FFI via ctypes)
=========================================

Usage::

    from turbodb import Database

    db = Database("./mydata")
    users = db.collection("users")
    doc_id = users.insert("alice", '{"name": "Alice", "age": 30}')
    doc = users.get("alice")
    print(doc)  # {'key': 'alice', 'value': '{"name":"Alice","age":30}', 'doc_id': 1, 'version': 0}

    users.update("alice", '{"name": "Alice", "age": 31}')
    users.delete("alice")

    # Scan
    for doc in users.scan(limit=100):
        print(doc)

    db.close()

    # Or use as context manager:
    with Database("./mydata") as db:
        col = db.collection("users")
        col.insert("bob", '{"name": "Bob"}')
"""

from . import _ffi
import json
import ctypes

__version__ = _ffi.version()
__all__ = ["Database", "Collection", "VectorColumn", "crypto", "__version__"]


class Collection:
    """A TurboDB document collection."""

    def __init__(self, db_handle, name: str, tenant: str | None = None):
        self._db = db_handle
        self._name = name
        self._tenant = tenant
        self._handle = (
            _ffi.get_collection_for_tenant(db_handle, tenant, name)
            if tenant is not None else
            _ffi.get_collection(db_handle, name)
        )

    @property
    def name(self) -> str:
        return self._name

    def insert(self, key: str, value) -> int:
        """Insert a document. `value` can be a string or a dict (auto-serialized to JSON).
        Returns the assigned doc_id."""
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        return _ffi.insert(self._handle, key, str(value))

    def get(self, key: str):
        """Get a document by key. Returns a dict or None."""
        result = _ffi.get(self._handle, key)
        if result is None:
            return None
        return {
            "key": result.key_bytes().decode("utf-8", errors="replace"),
            "value": result.val_bytes().decode("utf-8", errors="replace"),
            "doc_id": result.doc_id,
            "version": result.version,
        }

    def update(self, key: str, value) -> bool:
        """Update a document. Returns True if found and updated."""
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        return _ffi.update(self._handle, key, str(value))

    def delete(self, key: str) -> bool:
        """Delete a document. Returns True if found and deleted."""
        return _ffi.delete(self._handle, key)

    def scan(self, limit: int = 20, offset: int = 0):
        """Scan documents. Returns a list of dicts."""
        handle = _ffi.scan(self._handle, limit, offset)
        try:
            count = _ffi.scan_count(handle)
            docs = []
            for i in range(count):
                r = _ffi.scan_doc(handle, i)
                if r is not None:
                    docs.append({
                        "key": r.key_bytes().decode("utf-8", errors="replace"),
                        "value": r.val_bytes().decode("utf-8", errors="replace"),
                        "doc_id": r.doc_id,
                        "version": r.version,
                    })
            return docs
        finally:
            _ffi.scan_free(handle)

    def search(self, query: str, limit: int = 20):
        """Full-text search using trigram index. Returns a list of matching docs."""
        qb = query.encode("utf-8")
        handle = _ffi.ScanHandle()
        rc = _ffi._lib.turbodb_search(self._handle, qb, len(qb), limit, ctypes.byref(handle))
        if rc != 0:
            return []
        try:
            count = _ffi.scan_count(handle)
            docs = []
            for i in range(count):
                r = _ffi.scan_doc(handle, i)
                if r is not None:
                    docs.append({
                        "key": r.key_bytes().decode("utf-8", errors="replace"),
                        "value": r.val_bytes().decode("utf-8", errors="replace"),
                        "doc_id": r.doc_id,
                        "version": r.version,
                    })
            return docs
        finally:
            _ffi.scan_free(handle)

    def flush_index(self):
        """Block until background index builder has finished processing all pending items."""
        _ffi._lib.turbodb_flush_index(self._handle)

    def configure_vectors(self, dims: int, field: str = "embedding"):
        """Configure a vector embedding column on this collection.
        `dims` is the dimensionality; `field` is the JSON key holding the embedding array."""
        fb = field.encode("utf-8")
        rc = _ffi._lib.turbodb_configure_vectors(self._handle, dims, fb, len(fb))
        if rc != 0:
            raise RuntimeError("Failed to configure vectors")

    def search_vectors(self, query, k: int = 10, metric: str = "cosine"):
        """Search the collection's vector column. Returns list of (index, score) tuples."""
        import ctypes
        metric_map = {"cosine": 0, "dot_product": 1, "l2": 2}
        m = metric_map.get(metric, 0)
        dims = len(query)
        arr = (ctypes.c_float * dims)(*query)
        out_idx = (ctypes.c_uint32 * k)()
        out_scores = (ctypes.c_float * k)()
        rc = _ffi._lib.turbodb_collection_search_vectors(
            self._handle, arr, dims, k, m, out_idx, out_scores
        )
        if rc < 0:
            return []
        return [(int(out_idx[i]), float(out_scores[i])) for i in range(rc)]

    def search_hybrid(self, text_query: str, vector_query, k: int = 10,
                      text_weight: float = 0.3, vector_weight: float = 0.7):
        """Hybrid search combining text + vector scoring.
        Returns a list of matching docs ranked by combined score."""
        import ctypes
        tb = text_query.encode("utf-8")
        dims = len(vector_query)
        arr = (ctypes.c_float * dims)(*vector_query)
        handle = _ffi.ScanHandle()
        rc = _ffi._lib.turbodb_search_hybrid(
            self._handle, tb, len(tb), arr, dims, k,
            ctypes.c_float(text_weight), ctypes.c_float(vector_weight),
            ctypes.byref(handle),
        )
        if rc != 0:
            return []
        try:
            count = _ffi.scan_count(handle)
            docs = []
            for i in range(count):
                r = _ffi.scan_doc(handle, i)
                if r is not None:
                    docs.append({
                        "key": r.key_bytes().decode("utf-8", errors="replace"),
                        "value": r.val_bytes().decode("utf-8", errors="replace"),
                        "doc_id": r.doc_id,
                        "version": r.version,
                    })
            return docs
        finally:
            _ffi.scan_free(handle)
    def __repr__(self):
        if self._tenant is None:
            return f"Collection('{self._name}')"
        return f"Collection('{self._tenant}/{self._name}')"


class Database:
    """A TurboDB database instance."""

    def __init__(self, path: str = "./turbodb_data"):
        self._path = path
        self._handle = _ffi.open_db(path)
        self._collections = {}

    def collection(self, name: str) -> Collection:
        """Get or create a named collection."""
        if name not in self._collections:
            self._collections[name] = Collection(self._handle, name)
        return self._collections[name]

    def tenant(self, tenant_id: str):
        return TenantDatabase(self, tenant_id)

    def drop_collection(self, name: str):
        """Drop a collection."""
        _ffi.drop_collection(self._handle, name)
        self._collections.pop(name, None)

    def close(self):
        """Close the database and free all resources."""
        if self._handle:
            _ffi.close_db(self._handle)
            self._handle = None
            self._collections.clear()

    @property
    def path(self) -> str:
        return self._path

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    def __repr__(self):
        return f"Database('{self._path}')"


class TenantDatabase:
    def __init__(self, db: Database, tenant_id: str):
        self._db = db
        self._tenant_id = tenant_id
        self._collections = {}

    def collection(self, name: str) -> Collection:
        cache_key = f"{self._tenant_id}/{name}"
        if cache_key not in self._collections:
            self._collections[cache_key] = Collection(self._db._handle, name, tenant=self._tenant_id)
        return self._collections[cache_key]

    def drop_collection(self, name: str):
        _ffi.drop_collection_for_tenant(self._db._handle, self._tenant_id, name)
        self._collections.pop(f"{self._tenant_id}/{name}", None)


class VectorColumn:
    """A SIMD-accelerated vector column for similarity search.

    Usage::

        from turbodb import VectorColumn

        col = VectorColumn(dims=128)
        col.append([0.1] * 128)
        col.append([0.2] * 128)

        results = col.search([0.1] * 128, k=5, metric="cosine")
        for r in results:
            print(r["index"], r["score"])

        # Enable quantization for faster search
        col.enable_quantization(bit_width=4, seed=42)
        results = col.search_quantized([0.1] * 128, k=5, metric="cosine")

        col.close()
    """

    def __init__(self, dims: int):
        self._handle = _ffi._lib.turbodb_vector_create(dims)
        if not self._handle:
            raise RuntimeError(f"Failed to create VectorColumn with dims={dims}")
        self._dims = dims

    @property
    def dims(self) -> int:
        return self._dims

    def append(self, vector):
        """Append a vector (list or tuple of floats). Must match dims."""
        arr = (ctypes.c_float * len(vector))(*vector)
        rc = _ffi._lib.turbodb_vector_append(self._handle, arr, len(vector))
        if rc != 0:
            raise RuntimeError("Vector append failed (dimension mismatch?)")

    def search(self, query, k: int = 10, metric: str = "cosine"):
        """Brute-force FP32 search. Returns list of {index, score} dicts."""
        metric_map = {"cosine": 0, "dot_product": 1, "l2": 2}
        if metric not in metric_map:
            raise ValueError(f"Unknown metric '{metric}', use one of {list(metric_map)}")
        arr = (ctypes.c_float * len(query))(*query)
        indices = (ctypes.c_uint32 * k)()
        scores = (ctypes.c_float * k)()
        n = _ffi._lib.turbodb_vector_search(
            self._handle, arr, len(query), k, metric_map[metric], indices, scores
        )
        if n < 0:
            raise RuntimeError("Vector search failed")
        return [{"index": int(indices[i]), "score": float(scores[i])} for i in range(n)]

    def enable_quantization(self, bit_width: int = 4, seed: int = 42):
        """Enable TurboQuant quantization. Quantizes all existing vectors."""
        rc = _ffi._lib.turbodb_vector_enable_quantization(self._handle, bit_width, seed)
        if rc != 0:
            raise RuntimeError(f"Failed to enable quantization (bit_width={bit_width})")

    def search_quantized(self, query, k: int = 10, metric: str = "cosine"):
        """Quantized search: fast asymmetric scan + FP32 re-rank."""
        metric_map = {"cosine": 0, "dot_product": 1, "l2": 2}
        if metric not in metric_map:
            raise ValueError(f"Unknown metric '{metric}', use one of {list(metric_map)}")
        arr = (ctypes.c_float * len(query))(*query)
        indices = (ctypes.c_uint32 * k)()
        scores = (ctypes.c_float * k)()
        n = _ffi._lib.turbodb_vector_search_quantized(
            self._handle, arr, len(query), k, metric_map[metric], indices, scores
        )
        if n < 0:
            raise RuntimeError("Quantized search failed")
        return [{"index": int(indices[i]), "score": float(scores[i])} for i in range(n)]

    def enable_int8(self, seed: int = 42):
        """Enable INT8 SIMD direct distance computation. Quantizes all existing vectors to i8."""
        rc = _ffi._lib.turbodb_vector_enable_int8(self._handle, seed)
        if rc != 0:
            raise RuntimeError("Failed to enable INT8 mode")

    def search_int8(self, query, k: int = 10, metric: str = "cosine"):
        """INT8 SIMD search: 4x less memory, 16 i8 lanes vs 8 f32 lanes per SIMD op."""
        metric_map = {"cosine": 0, "dot_product": 1, "l2": 2}
        if metric not in metric_map:
            raise ValueError(f"Unknown metric '{metric}', use one of {list(metric_map)}")
        arr = (ctypes.c_float * len(query))(*query)
        indices = (ctypes.c_uint32 * k)()
        scores = (ctypes.c_float * k)()
        n = _ffi._lib.turbodb_vector_search_int8(
            self._handle, arr, len(query), k, metric_map[metric], indices, scores
        )
        if n < 0:
            raise RuntimeError("INT8 search failed")
        return [{"index": int(indices[i]), "score": float(scores[i])} for i in range(n)]

    def search_int8_parallel(self, query, k: int = 10, metric: str = "cosine"):
        """Parallel INT8 SIMD search: splits scan across multiple threads."""
        metric_map = {"cosine": 0, "dot_product": 1, "l2": 2}
        if metric not in metric_map:
            raise ValueError(f"Unknown metric '{metric}', use one of {list(metric_map)}")
        arr = (ctypes.c_float * len(query))(*query)
        indices = (ctypes.c_uint32 * k)()
        scores = (ctypes.c_float * k)()
        n = _ffi._lib.turbodb_vector_search_int8_parallel(
            self._handle, arr, len(query), k, metric_map[metric], indices, scores
        )
        if n < 0:
            raise RuntimeError("Parallel INT8 search failed")
        return [{"index": int(indices[i]), "score": float(scores[i])} for i in range(n)]

    def build_ivf(self, n_clusters: int = 16, seed: int = 42):
        """Build IVF (Inverted File Index) for approximate nearest neighbor search."""
        rc = _ffi._lib.turbodb_vector_build_ivf(self._handle, n_clusters, seed)
        if rc != 0:
            raise RuntimeError(f"Failed to build IVF (n_clusters={n_clusters})")

    def search_ivf(self, query, k: int = 10, metric: str = "cosine", n_probes: int = 4):
        """IVF search: probe only n_probes nearest clusters instead of full scan."""
        metric_map = {"cosine": 0, "dot_product": 1, "l2": 2}
        if metric not in metric_map:
            raise ValueError(f"Unknown metric '{metric}', use one of {list(metric_map)}")
        arr = (ctypes.c_float * len(query))(*query)
        indices = (ctypes.c_uint32 * k)()
        scores = (ctypes.c_float * k)()
        n = _ffi._lib.turbodb_vector_search_ivf(
            self._handle, arr, len(query), k, metric_map[metric], n_probes, indices, scores
        )
        if n < 0:
            raise RuntimeError("IVF search failed")
        return [{"index": int(indices[i]), "score": float(scores[i])} for i in range(n)]

    def count(self) -> int:
        """Number of vectors in the column."""
        return _ffi._lib.turbodb_vector_count(self._handle)

    def memory_bytes(self) -> int:
        """Total memory used by this column in bytes."""
        return _ffi._lib.turbodb_vector_memory(self._handle)

    def close(self):
        """Free the vector column."""
        if self._handle:
            _ffi._lib.turbodb_vector_free(self._handle)
            self._handle = None

    def __del__(self):
        self.close()

    def __repr__(self):
        count = self.count() if self._handle else 0
        return f"VectorColumn(dims={self._dims}, count={count})"


# Zero-dependency cryptographic functions powered by Zig's std.crypto.
# No OpenSSL, no pip install — just call them.

class crypto:
    """Cryptographic primitives via TurboDB's Zig core.

    Usage::

        from turbodb import crypto

        h = crypto.sha256(b"hello")        # bytes (32)
        h = crypto.sha256_hex(b"hello")    # "2cf24dba..."
        h = crypto.blake3(b"hello")        # bytes (32)
        h = crypto.blake3_hex(b"hello")    # hex string
        h = crypto.hmac_sha256(key, data)  # bytes (32)

        pub, sec = crypto.ed25519_keygen()
        sig = crypto.ed25519_sign(b"msg", sec)
        assert crypto.ed25519_verify(b"msg", sig, pub)
    """

    @staticmethod
    def sha256(data: bytes) -> bytes:
        """SHA-256 hash (32 bytes)."""
        out = ctypes.create_string_buffer(32)
        _ffi._lib.turbodb_sha256(data, len(data), out)
        return out.raw

    @staticmethod
    def sha256_hex(data: bytes) -> str:
        """SHA-256 hash as hex string (64 chars)."""
        out = ctypes.create_string_buffer(64)
        _ffi._lib.turbodb_sha256_hex(data, len(data), out)
        return out.raw.decode("ascii")

    @staticmethod
    def sha512(data: bytes) -> bytes:
        """SHA-512 hash (64 bytes)."""
        out = ctypes.create_string_buffer(64)
        _ffi._lib.turbodb_sha512(data, len(data), out)
        return out.raw

    @staticmethod
    def blake3(data: bytes) -> bytes:
        """BLAKE3 hash (32 bytes). Faster than SHA-256."""
        out = ctypes.create_string_buffer(32)
        _ffi._lib.turbodb_blake3(data, len(data), out)
        return out.raw

    @staticmethod
    def blake3_hex(data: bytes) -> str:
        """BLAKE3 hash as hex string (64 chars)."""
        out = ctypes.create_string_buffer(64)
        _ffi._lib.turbodb_blake3_hex(data, len(data), out)
        return out.raw.decode("ascii")

    @staticmethod
    def hmac_sha256(key: bytes, data: bytes) -> bytes:
        """HMAC-SHA256 (32 bytes). Use for API key derivation, webhooks."""
        out = ctypes.create_string_buffer(32)
        _ffi._lib.turbodb_hmac_sha256(key, len(key), data, len(data), out)
        return out.raw

    @staticmethod
    def hmac_sha256_hex(key: bytes, data: bytes) -> str:
        """HMAC-SHA256 as hex string (64 chars)."""
        out = ctypes.create_string_buffer(64)
        _ffi._lib.turbodb_hmac_sha256_hex(key, len(key), data, len(data), out)
        return out.raw.decode("ascii")

    @staticmethod
    def ed25519_keygen() -> tuple:
        """Generate Ed25519 keypair. Returns (public_key: bytes, secret_key: bytes)."""
        pub = ctypes.create_string_buffer(32)
        sec = ctypes.create_string_buffer(64)
        _ffi._lib.turbodb_ed25519_keygen(pub, sec)
        return pub.raw, sec.raw

    @staticmethod
    def ed25519_sign(message: bytes, secret_key: bytes) -> bytes:
        """Sign a message with Ed25519. Returns 64-byte signature."""
        sig = ctypes.create_string_buffer(64)
        _ffi._lib.turbodb_ed25519_sign(message, len(message), secret_key, sig)
        return sig.raw

    @staticmethod
    def ed25519_verify(message: bytes, signature: bytes, public_key: bytes) -> bool:
        """Verify an Ed25519 signature. Returns True if valid."""
        rc = _ffi._lib.turbodb_ed25519_verify(message, len(message), signature, public_key)
        return rc == 0
