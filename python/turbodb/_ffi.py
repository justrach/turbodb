"""Low-level ctypes bindings for libturbodb."""

import ctypes
import ctypes.util
import os
import platform
import sys

# ── Locate shared library ────────────────────────────────────────────────────

def _find_lib():
    """Find libturbodb.dylib / .so relative to this package or in zig-out."""
    # Common locations (ordered by priority)
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = []

    ext = ".dylib" if platform.system() == "Darwin" else ".so"
    name = f"libturbodb{ext}"

    # 1. Bundled alongside this package
    candidates.append(os.path.join(here, name))
    # 2. Project zig-out/lib
    candidates.append(os.path.join(here, "..", "..", "zig-out", "lib", name))
    # 3. System lib path
    found = ctypes.util.find_library("turbodb")
    if found:
        candidates.append(found)
    # 4. TURBODB_LIB env var
    env = os.environ.get("TURBODB_LIB")
    if env:
        candidates.insert(0, env)

    for path in candidates:
        path = os.path.abspath(path)
        if os.path.isfile(path):
            return ctypes.CDLL(path)

    raise OSError(
        f"Cannot find libturbodb{ext}. "
        f"Searched: {candidates}. "
        f"Set TURBODB_LIB=/path/to/libturbodb{ext} or run 'zig build' first."
    )


_lib = _find_lib()

# ── C struct mirrors ─────────────────────────────────────────────────────────

class DocResult(ctypes.Structure):
    _fields_ = [
        ("key_ptr",  ctypes.c_void_p),
        ("key_len",  ctypes.c_size_t),
        ("val_ptr",  ctypes.c_void_p),
        ("val_len",  ctypes.c_size_t),
        ("doc_id",   ctypes.c_uint64),
        ("version",  ctypes.c_uint8),
        ("_pad",     ctypes.c_uint8 * 7),
    ]

    def key_bytes(self) -> bytes:
        if not self.key_ptr or self.key_len == 0:
            return b""
        return ctypes.string_at(self.key_ptr, self.key_len)

    def val_bytes(self) -> bytes:
        if not self.val_ptr or self.val_len == 0:
            return b""
        return ctypes.string_at(self.val_ptr, self.val_len)


class ScanHandle(ctypes.Structure):
    _fields_ = [
        ("docs_ptr", ctypes.c_void_p),
        ("count",    ctypes.c_uint32),
        ("_pad",     ctypes.c_uint8 * 4),
    ]


# ── Function signatures ─────────────────────────────────────────────────────

# turbodb_open(dir, dir_len) -> *opaque
_lib.turbodb_open.argtypes = [ctypes.c_char_p, ctypes.c_size_t]
_lib.turbodb_open.restype = ctypes.c_void_p

# turbodb_close(handle)
_lib.turbodb_close.argtypes = [ctypes.c_void_p]
_lib.turbodb_close.restype = None

# turbodb_collection(db, name, name_len) -> *opaque
_lib.turbodb_collection.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
_lib.turbodb_collection.restype = ctypes.c_void_p

_lib.turbodb_collection_tenant.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t,
]
_lib.turbodb_collection_tenant.restype = ctypes.c_void_p

# turbodb_drop_collection(db, name, name_len)
_lib.turbodb_drop_collection.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
_lib.turbodb_drop_collection.restype = None

_lib.turbodb_drop_collection_tenant.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t,
]
_lib.turbodb_drop_collection_tenant.restype = None

# turbodb_insert(col, key, key_len, val, val_len, out_id) -> c_int
_lib.turbodb_insert.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_uint64),
]
_lib.turbodb_insert.restype = ctypes.c_int

# turbodb_insert_with_embedding(col, key, key_len, val, val_len, embedding, dims, out_id)
_lib.turbodb_insert_with_embedding.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_float), ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_uint64),
]
_lib.turbodb_insert_with_embedding.restype = ctypes.c_int

# turbodb_get(col, key, key_len, out) -> c_int
_lib.turbodb_get.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
    ctypes.POINTER(DocResult),
]
_lib.turbodb_get.restype = ctypes.c_int

# turbodb_update(col, key, key_len, val, val_len) -> c_int
_lib.turbodb_update.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t,
]
_lib.turbodb_update.restype = ctypes.c_int

# turbodb_delete(col, key, key_len) -> c_int
_lib.turbodb_delete.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
_lib.turbodb_delete.restype = ctypes.c_int

# turbodb_scan(col, limit, offset, out) -> c_int
_lib.turbodb_scan.argtypes = [
    ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32,
    ctypes.POINTER(ScanHandle),
]
_lib.turbodb_scan.restype = ctypes.c_int

# turbodb_scan_count(scan) -> u32
_lib.turbodb_scan_count.argtypes = [ctypes.POINTER(ScanHandle)]
_lib.turbodb_scan_count.restype = ctypes.c_uint32

# turbodb_scan_doc(scan, idx, out) -> c_int
_lib.turbodb_scan_doc.argtypes = [
    ctypes.POINTER(ScanHandle), ctypes.c_uint32,
    ctypes.POINTER(DocResult),
]
_lib.turbodb_scan_doc.restype = ctypes.c_int

# turbodb_scan_free(scan)
_lib.turbodb_scan_free.argtypes = [ctypes.POINTER(ScanHandle)]
_lib.turbodb_scan_free.restype = None

# turbodb_version() -> *c_char
_lib.turbodb_version.argtypes = []
_lib.turbodb_version.restype = ctypes.c_char_p

# turbodb_search(col, query, query_len, limit, out) -> c_int
_lib.turbodb_search.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_uint32, ctypes.POINTER(ScanHandle),
]
_lib.turbodb_search.restype = ctypes.c_int

# turbodb_flush_index(col) -> void
_lib.turbodb_flush_index.argtypes = [ctypes.c_void_p]
_lib.turbodb_flush_index.restype = None

# ── Crypto ───────────────────────────────────────────────────────────────────

# turbodb_sha256(data, len, out)
_lib.turbodb_sha256.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p]
_lib.turbodb_sha256.restype = None

# turbodb_sha256_hex(data, len, out)
_lib.turbodb_sha256_hex.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p]
_lib.turbodb_sha256_hex.restype = None

# turbodb_sha512(data, len, out)
_lib.turbodb_sha512.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p]
_lib.turbodb_sha512.restype = None

# turbodb_blake3(data, len, out)
_lib.turbodb_blake3.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p]
_lib.turbodb_blake3.restype = None

# turbodb_blake3_hex(data, len, out)
_lib.turbodb_blake3_hex.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p]
_lib.turbodb_blake3_hex.restype = None

# turbodb_hmac_sha256(key, klen, data, dlen, out)
_lib.turbodb_hmac_sha256.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p,
]
_lib.turbodb_hmac_sha256.restype = None

# turbodb_hmac_sha256_hex(key, klen, data, dlen, out)
_lib.turbodb_hmac_sha256_hex.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p,
]
_lib.turbodb_hmac_sha256_hex.restype = None

# turbodb_ed25519_keygen(pub_out, sec_out)
_lib.turbodb_ed25519_keygen.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
_lib.turbodb_ed25519_keygen.restype = None

# turbodb_ed25519_sign(msg, msg_len, sk, sig_out)
_lib.turbodb_ed25519_sign.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.c_char_p,
]
_lib.turbodb_ed25519_sign.restype = None

# turbodb_ed25519_verify(msg, msg_len, sig, pk) -> c_int
_lib.turbodb_ed25519_verify.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.c_char_p,
]
_lib.turbodb_ed25519_verify.restype = ctypes.c_int


# ── Vector column ────────────────────────────────────────────────────────────

# turbodb_vector_create(dims) -> *opaque
_lib.turbodb_vector_create.argtypes = [ctypes.c_uint32]
_lib.turbodb_vector_create.restype = ctypes.c_void_p

# turbodb_vector_free(handle)
_lib.turbodb_vector_free.argtypes = [ctypes.c_void_p]
_lib.turbodb_vector_free.restype = None

# turbodb_vector_append(handle, data, dims) -> c_int
_lib.turbodb_vector_append.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32]
_lib.turbodb_vector_append.restype = ctypes.c_int

# turbodb_vector_search(handle, query, dims, k, metric, out_indices, out_scores) -> c_int
_lib.turbodb_vector_search.argtypes = [
    ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32,
    ctypes.c_uint32, ctypes.c_uint8, ctypes.POINTER(ctypes.c_uint32),
    ctypes.POINTER(ctypes.c_float),
]
_lib.turbodb_vector_search.restype = ctypes.c_int

# turbodb_vector_count(handle) -> u32
_lib.turbodb_vector_count.argtypes = [ctypes.c_void_p]
_lib.turbodb_vector_count.restype = ctypes.c_uint32

# turbodb_vector_memory(handle) -> usize
_lib.turbodb_vector_memory.argtypes = [ctypes.c_void_p]
_lib.turbodb_vector_memory.restype = ctypes.c_size_t

# turbodb_vector_enable_quantization(handle, bit_width, seed) -> c_int
_lib.turbodb_vector_enable_quantization.argtypes = [ctypes.c_void_p, ctypes.c_uint8, ctypes.c_uint64]
_lib.turbodb_vector_enable_quantization.restype = ctypes.c_int

# turbodb_vector_search_quantized(handle, query, dims, k, metric, out_indices, out_scores) -> c_int
_lib.turbodb_vector_search_quantized.argtypes = [
    ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32,
    ctypes.c_uint32, ctypes.c_uint8, ctypes.POINTER(ctypes.c_uint32),
    ctypes.POINTER(ctypes.c_float),
]
_lib.turbodb_vector_search_quantized.restype = ctypes.c_int

# turbodb_vector_enable_int8(handle, seed) -> c_int
_lib.turbodb_vector_enable_int8.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
_lib.turbodb_vector_enable_int8.restype = ctypes.c_int

# turbodb_vector_search_int8(handle, query, dims, k, metric, out_indices, out_scores) -> c_int
_lib.turbodb_vector_search_int8.argtypes = [
    ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32,
    ctypes.c_uint32, ctypes.c_uint8, ctypes.POINTER(ctypes.c_uint32),
    ctypes.POINTER(ctypes.c_float),
]
_lib.turbodb_vector_search_int8.restype = ctypes.c_int

# turbodb_vector_search_int8_parallel(handle, query, dims, k, metric, out_indices, out_scores) -> c_int
_lib.turbodb_vector_search_int8_parallel.argtypes = [
    ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32,
    ctypes.c_uint32, ctypes.c_uint8, ctypes.POINTER(ctypes.c_uint32),
    ctypes.POINTER(ctypes.c_float),
]
_lib.turbodb_vector_search_int8_parallel.restype = ctypes.c_int

# turbodb_vector_build_ivf(handle, n_clusters, seed) -> c_int
_lib.turbodb_vector_build_ivf.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint64]
_lib.turbodb_vector_build_ivf.restype = ctypes.c_int

# turbodb_vector_search_ivf(handle, query, dims, k, metric, n_probes, out_indices, out_scores) -> c_int
_lib.turbodb_vector_search_ivf.argtypes = [
    ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32,
    ctypes.c_uint32, ctypes.c_uint8, ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_uint32), ctypes.POINTER(ctypes.c_float),
]
_lib.turbodb_vector_search_ivf.restype = ctypes.c_int

# ── Collection-level vector integration ──────────────────────────────────────

# turbodb_configure_vectors(col, dims, field_name, field_len) -> c_int
_lib.turbodb_configure_vectors.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_char_p, ctypes.c_uint32]
_lib.turbodb_configure_vectors.restype = ctypes.c_int

# turbodb_collection_search_vectors(col, query, dims, k, metric, out_indices, out_scores) -> c_int
_lib.turbodb_collection_search_vectors.argtypes = [
    ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32,
    ctypes.c_uint32, ctypes.c_uint8, ctypes.POINTER(ctypes.c_uint32),
    ctypes.POINTER(ctypes.c_float),
]
_lib.turbodb_collection_search_vectors.restype = ctypes.c_int

# turbodb_search_hybrid(col, text, text_len, vec_query, dims, k, text_w, vec_w, out) -> c_int
_lib.turbodb_search_hybrid.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_float), ctypes.c_uint32,
    ctypes.c_uint32, ctypes.c_float, ctypes.c_float,
    ctypes.POINTER(ScanHandle),
]
_lib.turbodb_search_hybrid.restype = ctypes.c_int
# ── Public low-level API ────────────────────────────────────────────────────

def open_db(path: str):
    b = path.encode("utf-8")
    h = _lib.turbodb_open(b, len(b))
    if not h:
        raise RuntimeError(f"Failed to open TurboDB at {path}")
    return h

def close_db(handle):
    _lib.turbodb_close(handle)

def get_collection(db_handle, name: str):
    b = name.encode("utf-8")
    h = _lib.turbodb_collection(db_handle, b, len(b))
    if not h:
        raise RuntimeError(f"Failed to open collection '{name}'")
    return h

def get_collection_for_tenant(db_handle, tenant: str, name: str):
    tb = tenant.encode("utf-8")
    nb = name.encode("utf-8")
    h = _lib.turbodb_collection_tenant(db_handle, tb, len(tb), nb, len(nb))
    if not h:
        raise RuntimeError(f"Failed to open collection '{tenant}/{name}'")
    return h

def drop_collection(db_handle, name: str):
    b = name.encode("utf-8")
    _lib.turbodb_drop_collection(db_handle, b, len(b))

def drop_collection_for_tenant(db_handle, tenant: str, name: str):
    tb = tenant.encode("utf-8")
    nb = name.encode("utf-8")
    _lib.turbodb_drop_collection_tenant(db_handle, tb, len(tb), nb, len(nb))

def insert(col_handle, key: str, value: str) -> int:
    kb = key.encode("utf-8")
    vb = value.encode("utf-8")
    out = ctypes.c_uint64(0)
    rc = _lib.turbodb_insert(col_handle, kb, len(kb), vb, len(vb), ctypes.byref(out))
    if rc != 0:
        raise RuntimeError(f"Insert failed for key '{key}'")
    return out.value

def get(col_handle, key: str):
    kb = key.encode("utf-8")
    result = DocResult()
    rc = _lib.turbodb_get(col_handle, kb, len(kb), ctypes.byref(result))
    if rc != 0:
        return None
    return result

def update(col_handle, key: str, value: str) -> bool:
    kb = key.encode("utf-8")
    vb = value.encode("utf-8")
    rc = _lib.turbodb_update(col_handle, kb, len(kb), vb, len(vb))
    return rc == 0

def delete(col_handle, key: str) -> bool:
    kb = key.encode("utf-8")
    rc = _lib.turbodb_delete(col_handle, kb, len(kb))
    return rc == 0

def scan(col_handle, limit: int = 20, offset: int = 0):
    handle = ScanHandle()
    rc = _lib.turbodb_scan(col_handle, limit, offset, ctypes.byref(handle))
    if rc != 0:
        raise RuntimeError("Scan failed")
    return handle

def scan_count(handle) -> int:
    return _lib.turbodb_scan_count(ctypes.byref(handle))

def scan_doc(handle, idx: int):
    result = DocResult()
    rc = _lib.turbodb_scan_doc(ctypes.byref(handle), idx, ctypes.byref(result))
    if rc != 0:
        return None
    return result

def scan_free(handle):
    _lib.turbodb_scan_free(ctypes.byref(handle))

def version() -> str:
    return _lib.turbodb_version().decode("utf-8")

# ── Branch operations ──────────────────────────────────────────────────────

_lib.turbodb_enable_branching.argtypes = [ctypes.c_void_p]
_lib.turbodb_enable_branching.restype = ctypes.c_int

_lib.turbodb_create_branch.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32,
    ctypes.c_char_p, ctypes.c_uint32,
]
_lib.turbodb_create_branch.restype = ctypes.c_int

_lib.turbodb_branch_write.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32,
    ctypes.c_char_p, ctypes.c_uint32,
    ctypes.c_char_p, ctypes.c_uint32,
]
_lib.turbodb_branch_write.restype = ctypes.c_int

_lib.turbodb_branch_read.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32,
    ctypes.c_char_p, ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_uint32),
]
_lib.turbodb_branch_read.restype = ctypes.c_int

_lib.turbodb_branch_merge.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32]
_lib.turbodb_branch_merge.restype = ctypes.c_int

_lib.turbodb_branch_diff.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_uint32)]
_lib.turbodb_branch_diff.restype = ctypes.c_int

# turbodb_branch_search(col, branch, branch_len, query, query_len, limit, out) -> c_int
_lib.turbodb_branch_search.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32,
    ctypes.c_char_p, ctypes.c_uint32,
    ctypes.c_uint32, ctypes.POINTER(ScanHandle),
]
_lib.turbodb_branch_search.restype = ctypes.c_int

# turbodb_list_branches(col, out_json, out_len) -> c_int
_lib.turbodb_list_branches.argtypes = [
    ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_uint32),
]
_lib.turbodb_list_branches.restype = ctypes.c_int

# turbodb_free_json(ptr, len) -> void
_lib.turbodb_free_json.argtypes = [ctypes.c_char_p, ctypes.c_uint32]
_lib.turbodb_free_json.restype = None

# turbodb_discover_context(col, query, query_len, limit, out_json, out_len) -> c_int
_lib.turbodb_discover_context.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32, ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_uint32),
]
_lib.turbodb_discover_context.restype = ctypes.c_int
