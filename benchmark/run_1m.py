#!/usr/bin/env python3
"""TurboDB batch insert + INT8 parallel search at 1M scale, vs Zvec."""
import sys, time, os, shutil, ctypes
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))
import zvec, numpy as np
from turbodb import VectorColumn, _ffi

for name, args, res in [
    ("turbodb_vector_enable_int8", [ctypes.c_void_p, ctypes.c_uint64], ctypes.c_int),
    ("turbodb_vector_search_int8_parallel", [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32, ctypes.c_uint32, ctypes.c_uint8, ctypes.POINTER(ctypes.c_uint32), ctypes.POINTER(ctypes.c_float)], ctypes.c_int),
    ("turbodb_vector_append_batch", [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint32, ctypes.c_uint32], ctypes.c_int),
]:
    getattr(_ffi._lib, name).argtypes = args
    getattr(_ffi._lib, name).restype = res

np.random.seed(42)

def tdb_search(h, q, k):
    a = (ctypes.c_float * len(q))(*q)
    i = (ctypes.c_uint32 * k)()
    s = (ctypes.c_float * k)()
    return _ffi._lib.turbodb_vector_search_int8_parallel(h, a, len(q), k, 0, i, s)

print("=" * 85)
print("  TurboDB (batch insert + INT8 parallel) vs Zvec 0.3.0")
print("=" * 85, flush=True)

run_id = 0
for dims in [1536]:
    for n in [10_000, 100_000, 1_000_000]:
        run_id += 1
        print(f"\n--- dims={dims}, N={n:,} ---", flush=True)
        vecs = np.random.randn(n, dims).astype(np.float32)
        vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
        qs = np.random.randn(10, dims).astype(np.float32)
        qs /= np.linalg.norm(qs, axis=1, keepdims=True)
        ql = qs.tolist()

        # TurboDB batch insert
        col = VectorColumn(dims)
        flat = vecs.flatten()
        arr = (ctypes.c_float * len(flat))(*flat)
        t0 = time.perf_counter()
        batch_sz = 10000
        for start in range(0, n, batch_sz):
            bsz = min(batch_sz, n - start)
            offset = start * dims
            ptr = ctypes.cast(ctypes.addressof(arr) + offset * 4, ctypes.POINTER(ctypes.c_float))
            _ffi._lib.turbodb_vector_append_batch(col._handle, ptr, dims, bsz)
        ti = time.perf_counter() - t0

        t0 = time.perf_counter()
        _ffi._lib.turbodb_vector_enable_int8(col._handle, 42)
        tq = time.perf_counter() - t0

        t0 = time.perf_counter()
        for q in ql: tdb_search(col._handle, q, 10)
        ts = (time.perf_counter() - t0) / 10 * 1000
        print(f"  TurboDB:  insert {n/ti:>9,.0f}/s  quant {tq:.1f}s  search {ts:>7.1f} ms/q", flush=True)
        col.close()
        del arr

        # Zvec (skip at 1M)
        if n <= 100_000:
            zp = f"/tmp/zvec_run_{run_id}"
            sc = zvec.CollectionSchema(name="bench_col", vectors=zvec.VectorSchema("emb", zvec.DataType.VECTOR_FP32, dims))
            zc = zvec.create_and_open(path=zp, schema=sc)
            vl = vecs.tolist()
            t0 = time.perf_counter()
            b = []
            for i in range(n):
                b.append(zvec.Doc(id=str(i), vectors={"emb": vl[i]}))
                if len(b) >= 1000: zc.insert(b); b = []
            if b: zc.insert(b)
            zi = time.perf_counter() - t0
            t0 = time.perf_counter()
            for q in ql: zc.query(zvec.VectorQuery("emb", vector=q), topk=10)
            zs = (time.perf_counter() - t0) / 10 * 1000
            del zc; shutil.rmtree(zp, True)
            print(f"  Zvec:     insert {n/zi:>9,.0f}/s             search {zs:>7.1f} ms/q", flush=True)
            xi = (n/ti)/(n/zi)
            xs = zs/ts if ts > 0 else 0
            print(f"  >> Insert: TurboDB {xi:.1f}x | Search: TurboDB {xs:.1f}x faster", flush=True)

        del vecs

print("\n" + "=" * 85)
