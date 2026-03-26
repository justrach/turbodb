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

__version__ = _ffi.version()
__all__ = ["Database", "Collection", "__version__"]


class Collection:
    """A TurboDB document collection."""

    def __init__(self, db_handle, name: str):
        self._db = db_handle
        self._name = name
        self._handle = _ffi.get_collection(db_handle, name)

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

    def __repr__(self):
        return f"Collection('{self._name}')"


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
