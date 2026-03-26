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

# turbodb_drop_collection(db, name, name_len)
_lib.turbodb_drop_collection.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
_lib.turbodb_drop_collection.restype = None

# turbodb_insert(col, key, key_len, val, val_len, out_id) -> c_int
_lib.turbodb_insert.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_uint64),
]
_lib.turbodb_insert.restype = ctypes.c_int

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

def drop_collection(db_handle, name: str):
    b = name.encode("utf-8")
    _lib.turbodb_drop_collection(db_handle, b, len(b))

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
