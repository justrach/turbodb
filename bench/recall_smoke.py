#!/usr/bin/env python3
"""Quick recall smoke test — verifies search indexer works end-to-end.

Inserts 500 docs, waits for indexing, checks recall >= 80%.
Exits 0 on pass, 1 on fail. Runs in ~5 seconds.
"""
import sys, os, time, json, shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))
import turbodb

DB_PATH = "/tmp/turbodb_recall_smoke"
shutil.rmtree(DB_PATH, ignore_errors=True)

docs = [
    (f"doc_{i}", json.dumps({"title": f"Article about topic {i} with keywords alpha beta gamma",
                              "text": f"This is document number {i} discussing various subjects "
                                      f"including science technology art history and culture"}))
    for i in range(500)
]

# Unique searchable phrases per doc
search_docs = [
    ("earth_science", json.dumps({"title": "Earth Science Overview", "text": "geology plate tectonics earthquakes volcanoes"})),
    ("quantum_physics", json.dumps({"title": "Quantum Physics Guide", "text": "quantum mechanics wave particle duality entanglement"})),
    ("roman_history", json.dumps({"title": "Roman History", "text": "roman empire julius caesar gladiators colosseum"})),
    ("jazz_music", json.dumps({"title": "Jazz Music Theory", "text": "jazz improvisation saxophone trumpet bebop swing"})),
    ("machine_learning", json.dumps({"title": "Machine Learning Basics", "text": "neural networks gradient descent backpropagation training"})),
]

db = turbodb.Database(DB_PATH)
col = db.collection("smoke")

# Insert all docs
for key, val in docs + search_docs:
    col.insert(key, val)

# Wait for indexer to process
col.flush_index()
time.sleep(1)

# Test recall on the unique docs
queries = [
    ("plate tectonics", "earth_science"),
    ("quantum mechanics", "quantum_physics"),
    ("julius caesar", "roman_history"),
    ("jazz improvisation", "jazz_music"),
    ("neural networks", "machine_learning"),
]

found = 0
for query, expected_key in queries:
    results = col.search(query, limit=20)
    keys = [r["key"] for r in results]
    if expected_key in keys:
        found += 1
    else:
        print(f"  MISS: '{query}' -> expected '{expected_key}', got {keys[:5]}")

recall = found / len(queries) * 100
print(f"Recall: {found}/{len(queries)} ({recall:.0f}%)")

db.close()
shutil.rmtree(DB_PATH, ignore_errors=True)

if recall >= 80:
    print("PASS")
    sys.exit(0)
else:
    print("FAIL — recall below 80%")
    sys.exit(1)
