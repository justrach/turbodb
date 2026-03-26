#!/usr/bin/env node
/**
 * TurboDB Node.js FFI smoke test.
 */

const fs = require('fs');
const os = require('os');
const path = require('path');
const { Database } = require('./index');

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'turbodb_test_'));
console.log(`TurboDB Node.js smoke test (data: ${tmp})`);

try {
  const db = new Database(tmp);

  // ── Collection ──────────────────────────────────────────────────────
  const users = db.collection('users');
  console.log(`  collection: users`);

  // ── Insert ──────────────────────────────────────────────────────────
  const id1 = users.insert('alice', { name: 'Alice', age: 30 });
  const id2 = users.insert('bob', { name: 'Bob', age: 25 });
  const id3 = users.insert('carol', '{"name": "Carol", "age": 35}');
  console.log(`  inserted: alice=${id1}, bob=${id2}, carol=${id3}`);

  // ── Get ─────────────────────────────────────────────────────────────
  const doc = users.get('alice');
  console.log(`  get alice:`, doc);
  console.assert(doc !== null, 'alice should exist');
  console.assert(doc.key === 'alice', 'key should be alice');
  console.assert(doc.value.includes('Alice'), 'value should contain Alice');

  const miss = users.get('nobody');
  console.assert(miss === null, 'nobody should not exist');
  console.log('  get miss: null ✓');

  // ── Update ──────────────────────────────────────────────────────────
  const updated = users.update('alice', { name: 'Alice', age: 31 });
  console.assert(updated, 'update should succeed');
  const doc2 = users.get('alice');
  console.assert(doc2.value.includes('31'), 'age should be 31');
  console.log(`  update alice:`, doc2);

  // ── Delete ──────────────────────────────────────────────────────────
  const deleted = users.delete('bob');
  console.assert(deleted, 'delete should succeed');
  console.assert(users.get('bob') === null, 'bob should be gone');
  console.log('  delete bob: ✓');

  // ── Scan ────────────────────────────────────────────────────────────
  const docs = users.scan(100);
  console.log(`  scan: ${docs.length} docs`);
  console.assert(docs.length >= 2, 'should have at least 2 docs');
  docs.forEach(d => console.log(`    ${d.key}: ${d.value.slice(0, 50)}`));

  db.close();
  console.log('\n✅ All tests passed!');

} finally {
  fs.rmSync(tmp, { recursive: true, force: true });
}
