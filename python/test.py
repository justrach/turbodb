#!/usr/bin/env python3
"""TurboDB Python FFI smoke test."""

import os, sys, shutil, tempfile

# Add parent to path so we can import turbodb package
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from turbodb import Database

def main():
    tmp = tempfile.mkdtemp(prefix="turbodb_test_")
    print(f"TurboDB Python smoke test (data: {tmp})")

    try:
        from turbodb._ffi import version
        print(f"  libturbodb version: {version()}")

        db = Database(tmp)

        # ── Collection ────────────────────────────────────────────────────
        users = db.collection("users")
        print(f"  collection: {users}")

        # ── Insert ────────────────────────────────────────────────────────
        id1 = users.insert("alice", {"name": "Alice", "age": 30})
        id2 = users.insert("bob", {"name": "Bob", "age": 25})
        id3 = users.insert("carol", '{"name": "Carol", "age": 35}')
        print(f"  inserted: alice={id1}, bob={id2}, carol={id3}")
        assert id1 >= 1
        assert id2 >= 1

        # ── Get ───────────────────────────────────────────────────────────
        doc = users.get("alice")
        print(f"  get alice: {doc}")
        assert doc is not None
        assert doc["key"] == "alice"
        assert "Alice" in doc["value"]

        miss = users.get("nobody")
        assert miss is None
        print("  get miss: None ✓")

        # ── Update ────────────────────────────────────────────────────────
        ok = users.update("alice", {"name": "Alice", "age": 31})
        assert ok
        doc2 = users.get("alice")
        assert "31" in doc2["value"]
        print(f"  update alice: {doc2}")

        # ── Delete ────────────────────────────────────────────────────────
        ok = users.delete("bob")
        assert ok
        assert users.get("bob") is None
        print("  delete bob: ✓")

        # ── Scan ──────────────────────────────────────────────────────────
        docs = users.scan(limit=100)
        print(f"  scan: {len(docs)} docs")
        assert len(docs) >= 2  # alice + carol (bob deleted)
        for d in docs:
            print(f"    {d['key']}: {d['value'][:50]}")

        db.close()
        print("\n✅ All tests passed!")

    finally:
        shutil.rmtree(tmp, ignore_errors=True)

if __name__ == "__main__":
    main()
