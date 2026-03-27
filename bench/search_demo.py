#!/usr/bin/env python3
"""
TurboDB trigram search demo — index a real codebase and search it.
Usage: python3 bench/search_demo.py /path/to/codebase "search query"
"""
import http.client, json, os, sys, time, glob

B="\033[1m"; G="\033[32m"; C="\033[36m"; D="\033[2m"; Y="\033[33m"; Z="\033[0m"

class TDB:
    def __init__(self, host, port):
        self.h, self.p = host, port
        self._c = None
    def _conn(self):
        if not self._c:
            self._c = http.client.HTTPConnection(self.h, self.p, timeout=10)
            self._c.connect()
        return self._c
    def _req(self, method, path, body=None):
        hdrs = {"Connection": "keep-alive"}
        enc = body.encode() if body else None
        if enc:
            hdrs["Content-Length"] = str(len(enc))
            hdrs["Content-Type"] = "application/json"
        for attempt in range(2):
            try:
                self._conn().request(method, path, body=enc, headers=hdrs)
                resp = self._conn().getresponse()
                data = resp.read()
                return json.loads(data) if data else {}
            except Exception:
                try: self._c.close()
                except: pass
                self._c = None
                if attempt: return {}
    def insert(self, col, key, val): return self._req("POST", f"/db/{col}", json.dumps({"key":key,"value":val}))
    def search(self, col, q, limit=20): return self._req("GET", f"/search/{col}?q={q}&limit={limit}")
    def drop(self, col):
        try: self._req("DELETE", f"/db/{col}")
        except: pass
    def health(self):
        try: return self._req("GET", "/health").get("engine") == "TurboDB"
        except: return False
    def close(self):
        try: self._c and self._c.close()
        except: pass

def index_codebase(tdb, col, root, exts=(".ts",".js",".py",".zig",".go",".rs",".c",".h",".java",".tsx",".jsx")):
    """Walk a directory, insert each source file as a document."""
    files = []
    for ext in exts:
        files.extend(glob.glob(os.path.join(root, "**", f"*{ext}"), recursive=True))
    files.sort()

    print(f"  {D}Found {len(files)} source files{Z}")
    tdb.drop(col)

    t0 = time.perf_counter()
    indexed = 0
    skipped = 0
    for f in files:
        try:
            with open(f, "r", errors="replace") as fh:
                content = fh.read(32768)  # first 32KB
        except Exception:
            skipped += 1
            continue
        # Use relative path as key, content as value
        relpath = os.path.relpath(f, root)
        val = json.dumps({"file": relpath, "content": content[:8192]})  # trim for HTTP
        tdb.insert(col, relpath, val)
        indexed += 1
        if indexed % 500 == 0:
            elapsed = time.perf_counter() - t0
            rate = indexed / elapsed
            print(f"\r  {D}Indexed {indexed}/{len(files)} ({rate:.0f} docs/s)...{Z}", end="", flush=True)

    elapsed = time.perf_counter() - t0
    print(f"\r  {G}Indexed {indexed} files in {elapsed:.1f}s ({indexed/elapsed:.0f} docs/s){Z}  (skipped {skipped})")
    return indexed

def run_search(tdb, col, query):
    """Run a trigram search and display results."""
    t0 = time.perf_counter()
    result = tdb.search(col, query, limit=20)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    hits = result.get("hits", 0)
    candidates = result.get("candidates", 0)
    total = result.get("total_docs", 0)
    tris = result.get("trigrams", 0)
    results_list = result.get("results", [])

    selectivity = (candidates / total * 100) if total else 0

    print(f"\n  {B}Search: \"{query}\"{Z}")
    print(f"  {D}trigrams: {tris}  candidates: {candidates}/{total} ({selectivity:.1f}% scanned)  hits: {hits}  time: {elapsed_ms:.1f}ms{Z}")
    print()

    for i, r in enumerate(results_list[:10]):
        val = r.get("value", {})
        if isinstance(val, str):
            try: val = json.loads(val)
            except: val = {"file": "?", "content": val}
        filepath = val.get("file", r.get("key", "?"))
        content = val.get("content", "")

        # Find and highlight match context
        q_lower = query.lower()
        c_lower = content.lower()
        pos = c_lower.find(q_lower)
        if pos >= 0:
            start = max(0, pos - 40)
            end = min(len(content), pos + len(query) + 40)
            snippet = content[start:end].replace("\n", " ").strip()
            # Highlight
            q_start = pos - start
            q_end = q_start + len(query)
            highlighted = snippet[:q_start] + f"{G}{B}" + snippet[q_start:q_end] + f"{Z}" + snippet[q_end:]
        else:
            highlighted = content[:80].replace("\n", " ").strip()

        print(f"  {C}{i+1:2}.{Z} {Y}{filepath}{Z}")
        print(f"      {highlighted}")

    if hits > 10:
        print(f"\n  {D}... and {hits - 10} more results{Z}")

    return result


def main():
    import subprocess, argparse

    ap = argparse.ArgumentParser(description="TurboDB trigram search demo on real codebases")
    ap.add_argument("codebase", help="path to codebase directory")
    ap.add_argument("queries", nargs="*", help="search queries (interactive if omitted)")
    ap.add_argument("--port", type=int, default=27020)
    ap.add_argument("--col", default="code")
    ap.add_argument("--binary", default="./zig-out/bin/turbodb")
    ap.add_argument("--no-index", action="store_true", help="skip indexing (already indexed)")
    args = ap.parse_args()

    # Start TurboDB in HTTP mode
    data_dir = "/tmp/turbodb_search_demo"
    os.makedirs(data_dir, exist_ok=True)

    # Always start fresh — trigram index is in-memory
    subprocess.run(["pkill", "-f", f"turbodb.*{args.port}"],
                   capture_output=True, timeout=2)
    time.sleep(0.5)
    import shutil
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    print(f"  {D}Starting TurboDB (HTTP mode) on :{args.port}...{Z}", end=" ", flush=True)
    proc = subprocess.Popen(
        [args.binary, "--data", data_dir, "--port", str(args.port), "--http"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    tdb = TDB("127.0.0.1", args.port)
    for _ in range(60):
        time.sleep(0.1)
        if tdb.health(): break
    else:
        print(f"\033[31mFailed to start TurboDB\033[0m")
        sys.exit(1)
    print(f"{G}ready (pid {proc.pid}){Z}")
    try:
        # Index
        if not args.no_index:
            print(f"\n{B}Indexing codebase...{Z}")
            n = index_codebase(tdb, args.col, args.codebase)
            print(f"  {B}Total: {n} documents in trigram index{Z}")

        # Search
        if args.queries:
            for q in args.queries:
                run_search(tdb, args.col, q)
        else:
            # Interactive mode
            print(f"\n{B}Interactive search{Z} (Ctrl+C to exit)")
            while True:
                try:
                    q = input(f"\n  {C}search>{Z} ").strip()
                    if not q: continue
                    run_search(tdb, args.col, q)
                except (KeyboardInterrupt, EOFError):
                    print()
                    break

    finally:
        tdb.close()
        if proc:
            proc.terminate()
            proc.wait()
if __name__ == "__main__":
    main()
