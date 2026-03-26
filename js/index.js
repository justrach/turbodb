/**
 * TurboDB — Node.js client (FFI via koffi)
 *
 * Usage:
 *   const { Database } = require('turbodb');
 *   const db = new Database('./mydata');
 *   const users = db.collection('users');
 *   const id = users.insert('alice', JSON.stringify({name: 'Alice'}));
 *   const doc = users.get('alice');
 *   db.close();
 */

const path = require('path');
const os = require('os');

let koffi;
try {
  koffi = require('koffi');
} catch {
  throw new Error(
    'koffi is required: npm install koffi\n' +
    'koffi is a zero-dependency native FFI library for Node.js'
  );
}

// ── Find shared library ─────────────────────────────────────────────────────

function findLib() {
  const ext = os.platform() === 'darwin' ? '.dylib' : '.so';
  const name = `libturbodb${ext}`;

  const candidates = [
    process.env.TURBODB_LIB,
    path.join(__dirname, name),
    path.join(__dirname, '..', 'zig-out', 'lib', name),
    path.join(__dirname, '..', '..', 'zig-out', 'lib', name),
  ].filter(Boolean);

  for (const p of candidates) {
    try {
      const fs = require('fs');
      if (fs.existsSync(p)) return p;
    } catch {}
  }

  throw new Error(
    `Cannot find ${name}. Searched: ${candidates.join(', ')}. ` +
    `Set TURBODB_LIB=/path/to/${name} or run 'zig build' first.`
  );
}

// ── Load library and define types ───────────────────────────────────────────

const libPath = findLib();
const lib = koffi.load(libPath);

const DocResult = koffi.struct('DocResult', {
  key_ptr:  'void *',
  key_len:  'size_t',
  val_ptr:  'void *',
  val_len:  'size_t',
  doc_id:   'uint64_t',
  version:  'uint8_t',
  _pad:     koffi.array('uint8_t', 7),
});

const ScanHandle = koffi.struct('ScanHandle', {
  docs_ptr: 'void *',
  count:    'uint32_t',
  _pad:     koffi.array('uint8_t', 4),
});

// ── Bind functions ──────────────────────────────────────────────────────────

const ffi = {
  open:            lib.func('void *turbodb_open(const char *dir, size_t dir_len)'),
  close:           lib.func('void turbodb_close(void *handle)'),
  collection:      lib.func('void *turbodb_collection(void *db, const char *name, size_t name_len)'),
  drop_collection: lib.func('void turbodb_drop_collection(void *db, const char *name, size_t name_len)'),
  insert:          lib.func('int turbodb_insert(void *col, const char *key, size_t key_len, const char *val, size_t val_len, uint64_t *out_id)'),
  get:             lib.func('int turbodb_get(void *col, const char *key, size_t key_len, DocResult *out)'),
  update:          lib.func('int turbodb_update(void *col, const char *key, size_t key_len, const char *val, size_t val_len)'),
  delete:          lib.func('int turbodb_delete(void *col, const char *key, size_t key_len)'),
  scan:            lib.func('int turbodb_scan(void *col, uint32_t limit, uint32_t offset, ScanHandle *out)'),
  scan_count:      lib.func('uint32_t turbodb_scan_count(ScanHandle *scan)'),
  scan_doc:        lib.func('int turbodb_scan_doc(ScanHandle *scan, uint32_t idx, DocResult *out)'),
  scan_free:       lib.func('void turbodb_scan_free(ScanHandle *scan)'),
  version:         lib.func('const char *turbodb_version()'),
};

// ── Helper: read C pointer+len as JS string ─────────────────────────────────

function ptrToString(ptr, len) {
  if (!ptr || len === 0) return '';
  const buf = koffi.decode(ptr, koffi.array('uint8_t', len));
  return Buffer.from(buf).toString('utf-8');
}

// ── Collection class ────────────────────────────────────────────────────────

class Collection {
  constructor(dbHandle, name) {
    this._db = dbHandle;
    this._name = name;
    const nameBuf = Buffer.from(name, 'utf-8');
    this._handle = ffi.collection(dbHandle, nameBuf, nameBuf.length);
    if (!this._handle) throw new Error(`Failed to open collection '${name}'`);
  }

  get name() { return this._name; }

  /**
   * Insert a document. Value can be string or object (auto-serialized).
   * @returns {number} doc_id
   */
  insert(key, value) {
    if (typeof value === 'object') value = JSON.stringify(value);
    const kb = Buffer.from(key, 'utf-8');
    const vb = Buffer.from(String(value), 'utf-8');
    const outId = [0];  // koffi uses array for out-params
    const rc = ffi.insert(this._handle, kb, kb.length, vb, vb.length, outId);
    if (rc !== 0) throw new Error(`Insert failed for key '${key}'`);
    return outId[0];
  }

  /**
   * Get a document by key. Returns {key, value, doc_id, version} or null.
   */
  get(key) {
    const kb = Buffer.from(key, 'utf-8');
    const result = {};
    const rc = ffi.get(this._handle, kb, kb.length, result);
    if (rc !== 0) return null;
    return {
      key: ptrToString(result.key_ptr, result.key_len),
      value: ptrToString(result.val_ptr, result.val_len),
      doc_id: result.doc_id,
      version: result.version,
    };
  }

  /**
   * Update a document. Returns true if found and updated.
   */
  update(key, value) {
    if (typeof value === 'object') value = JSON.stringify(value);
    const kb = Buffer.from(key, 'utf-8');
    const vb = Buffer.from(String(value), 'utf-8');
    return ffi.update(this._handle, kb, kb.length, vb, vb.length) === 0;
  }

  /**
   * Delete a document. Returns true if found and deleted.
   */
  delete(key) {
    const kb = Buffer.from(key, 'utf-8');
    return ffi.delete(this._handle, kb, kb.length) === 0;
  }

  /**
   * Scan documents. Returns array of {key, value, doc_id, version}.
   */
  scan(limit = 20, offset = 0) {
    const handle = {};
    const rc = ffi.scan(this._handle, limit, offset, handle);
    if (rc !== 0) throw new Error('Scan failed');

    try {
      const count = ffi.scan_count(handle);
      const docs = [];
      for (let i = 0; i < count; i++) {
        const docResult = {};
        if (ffi.scan_doc(handle, i, docResult) === 0) {
          docs.push({
            key: ptrToString(docResult.key_ptr, docResult.key_len),
            value: ptrToString(docResult.val_ptr, docResult.val_len),
            doc_id: docResult.doc_id,
            version: docResult.version,
          });
        }
      }
      return docs;
    } finally {
      ffi.scan_free(handle);
    }
  }
}

// ── Database class ──────────────────────────────────────────────────────────

class Database {
  constructor(dataPath = './turbodb_data') {
    this._path = dataPath;
    const pathBuf = Buffer.from(dataPath, 'utf-8');
    this._handle = ffi.open(pathBuf, pathBuf.length);
    if (!this._handle) throw new Error(`Failed to open TurboDB at '${dataPath}'`);
    this._collections = {};
  }

  get path() { return this._path; }

  collection(name) {
    if (!this._collections[name]) {
      this._collections[name] = new Collection(this._handle, name);
    }
    return this._collections[name];
  }

  dropCollection(name) {
    const nameBuf = Buffer.from(name, 'utf-8');
    ffi.drop_collection(this._handle, nameBuf, nameBuf.length);
    delete this._collections[name];
  }

  close() {
    if (this._handle) {
      ffi.close(this._handle);
      this._handle = null;
      this._collections = {};
    }
  }
}

// ── Exports ─────────────────────────────────────────────────────────────────

module.exports = { Database, Collection, version: ffi.version };
