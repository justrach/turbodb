#!/usr/bin/env python3
"""
TurboDB Trigram Search Demo
============================
Demonstrates Cursor-style trigram indexing over TurboDB documents.

Creates 50K documents (code snippets, log entries, config files),
builds a trigram index, and benchmarks indexed search vs linear scan.

Usage:
  python3 examples/search_demo.py              # default 50K docs
  python3 examples/search_demo.py --docs 200000  # 200K docs
"""

import argparse
import os
import random
import string
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))
from turbodb.search import TrigramIndex, extract_trigrams

# ── ANSI ──────────────────────────────────────────────────────────────────────

B = "\033[1m"
G = "\033[32m"
C = "\033[36m"
D = "\033[2m"
R = "\033[31m"
Y = "\033[33m"
Z = "\033[0m"

# ── Realistic document generators ────────────────────────────────────────────

LANGUAGES = ["python", "javascript", "rust", "go", "zig", "typescript", "java", "ruby"]
LOG_LEVELS = ["INFO", "WARN", "ERROR", "DEBUG", "FATAL"]
HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
STATUS_CODES = [200, 201, 301, 400, 401, 403, 404, 500, 502, 503]

FUNC_NAMES = [
    "handleRequest", "processPayment", "validateToken", "parseConfig",
    "fetchUserData", "updateInventory", "sendNotification", "calculateTax",
    "renderTemplate", "compressImage", "encryptPayload", "decryptMessage",
    "scheduleJob", "retryOperation", "loadBalancer", "rateLimit",
    "authenticateUser", "authorizeAccess", "logMetrics", "healthCheck",
    "initDatabase", "migrateSchema", "seedTestData", "cleanupExpired",
    "generateReport", "exportCSV", "importJSON", "transformData",
    "cacheInvalidate", "publishEvent", "consumeMessage", "routeTraffic",
    "MAX_FILE_SIZE", "MIN_BUFFER_LEN", "DEFAULT_TIMEOUT", "RETRY_COUNT",
    "CONNECTION_POOL_SIZE", "BATCH_LIMIT", "CACHE_TTL_SECONDS",
]

VAR_NAMES = [
    "user_id", "session_token", "request_body", "response_headers",
    "database_url", "api_key", "max_retries", "timeout_ms",
    "bucket_name", "queue_depth", "thread_count", "memory_limit",
    "config_path", "log_level", "cert_file", "private_key",
]

ERROR_MSGS = [
    "connection refused", "timeout exceeded", "invalid token",
    "permission denied", "resource not found", "rate limit exceeded",
    "disk quota exceeded", "out of memory", "deadlock detected",
    "checksum mismatch", "schema validation failed", "duplicate key",
    "certificate expired", "DNS resolution failed", "socket hang up",
]

IMPORTS = {
    "python": ["import os", "import sys", "import json", "from pathlib import Path",
               "import asyncio", "from typing import Optional", "import dataclasses"],
    "javascript": ["const express = require('express')", "import React from 'react'",
                   "const { Pool } = require('pg')", "import axios from 'axios'"],
    "rust": ["use std::collections::HashMap", "use tokio::sync::Mutex",
             "use serde::{Serialize, Deserialize}", "use anyhow::Result"],
    "go": ["import \"net/http\"", "import \"encoding/json\"",
           "import \"database/sql\"", "import \"context\""],
    "zig": ["const std = @import(\"std\")", "const mem = std.mem",
            "const Allocator = std.mem.Allocator"],
}


def gen_code_snippet():
    lang = random.choice(LANGUAGES)
    func = random.choice(FUNC_NAMES)
    var = random.choice(VAR_NAMES)
    imp = random.choice(IMPORTS.get(lang, ["import x"]))

    if lang == "python":
        return f"""{imp}

def {func}({var}: str, timeout: int = 30) -> dict:
    \"\"\"Process {var} with {func}.\"\"\"
    if not {var}:
        raise ValueError("missing {var}")
    result = fetch_remote(url=f"https://api.example.com/{{{{var}}}}", timeout=timeout)
    logger.info(f"{func} completed for {{{var}}}")
    return {{"status": "ok", "data": result}}
"""
    elif lang == "javascript":
        return f"""{imp}

async function {func}({var}, options = {{}}) {{
  const {{ timeout = 5000, retries = 3 }} = options;
  try {{
    const response = await fetch(`https://api.example.com/${{{var}}}`, {{
      method: 'POST',
      headers: {{ 'Authorization': `Bearer ${{token}}` }},
      signal: AbortSignal.timeout(timeout),
    }});
    if (!response.ok) throw new Error(`{func} failed: ${{response.status}}`);
    return await response.json();
  }} catch (err) {{
    console.error(`{func} error:`, err.message);
    throw err;
  }}
}}
"""
    elif lang == "rust":
        return f"""{imp}

pub async fn {func}({var}: &str, config: &Config) -> Result<Response> {{
    let client = reqwest::Client::new();
    let url = format!("https://api.example.com/{{}}", {var});
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {{}}", config.token))
        .timeout(Duration::from_secs(config.timeout))
        .send()
        .await?;
    if !resp.status().is_success() {{
        anyhow::bail!("{func} failed: {{}}", resp.status());
    }}
    Ok(resp.json().await?)
}}
"""
    elif lang == "go":
        return f"""{imp}

func {func}(ctx context.Context, {var} string) (*Response, error) {{
    req, err := http.NewRequestWithContext(ctx, "POST",
        fmt.Sprintf("https://api.example.com/%s", {var}), nil)
    if err != nil {{
        return nil, fmt.Errorf("{func}: %w", err)
    }}
    req.Header.Set("Authorization", "Bearer "+token)
    resp, err := http.DefaultClient.Do(req)
    if err != nil {{
        return nil, fmt.Errorf("{func}: %w", err)
    }}
    defer resp.Body.Close()
    return decodeResponse(resp)
}}
"""
    else:
        return f"// {lang}: {func}({var})\n// TODO: implement\n"


def gen_log_entry():
    level = random.choice(LOG_LEVELS)
    func = random.choice(FUNC_NAMES)
    ts = f"2026-03-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}.{random.randint(0,999):03d}Z"
    method = random.choice(HTTP_METHODS)
    status = random.choice(STATUS_CODES)
    path = f"/api/v{random.randint(1,3)}/{random.choice(['users','orders','products','sessions','webhooks'])}"
    latency = random.randint(1, 5000)
    
    if level in ("ERROR", "FATAL"):
        err = random.choice(ERROR_MSGS)
        return f"[{ts}] {level} {func}: {err} | method={method} path={path} status={status} latency={latency}ms"
    else:
        return f"[{ts}] {level} {func}: {method} {path} -> {status} ({latency}ms) user={random.randint(1000,99999)}"


def gen_config():
    entries = []
    for _ in range(random.randint(5, 15)):
        key = random.choice(VAR_NAMES + [f.lower() for f in FUNC_NAMES[:10]])
        val = random.choice([
            str(random.randint(1, 65535)),
            f'"{random.choice(["localhost", "0.0.0.0", "redis://cache:6379", "postgres://db:5432/app"])}"',
            random.choice(["true", "false"]),
            f'"{random.choice(ERROR_MSGS)}"',
        ])
        entries.append(f'{key} = {val}')
    return "\n".join(entries)


def generate_doc(i):
    """Generate a realistic document."""
    kind = random.choices(["code", "log", "config"], weights=[50, 40, 10])[0]
    if kind == "code":
        return f"code_{i:08d}", gen_code_snippet()
    elif kind == "log":
        return f"log_{i:08d}", gen_log_entry()
    else:
        return f"config_{i:08d}", gen_config()


# ── Demo ──────────────────────────────────────────────────────────────────────

def fmt(n):
    if n >= 1_000_000: return f"{n/1e6:.2f}M"
    if n >= 1_000: return f"{n/1e3:.1f}K"
    return f"{n:.0f}"

def fmt_us(us):
    if us >= 1_000_000: return f"{us/1e6:.2f}s"
    if us >= 1_000: return f"{us/1e3:.1f}ms"
    return f"{us:.0f}us"


def run_comparison(idx, label, query, is_regex=False):
    """Run indexed vs linear search and print comparison."""
    if is_regex:
        indexed = idx.regex_search(query)
        linear = idx.linear_regex_search(query)
    else:
        indexed = idx.search(query)
        linear = idx.linear_search(query)
    
    speedup = linear.elapsed_us / indexed.elapsed_us if indexed.elapsed_us > 0 else 0
    
    print(f"\n  {B}{label}{Z}: {C}{query}{Z}")
    print(f"    {'Method':<20} {'Hits':>6} {'Scanned':>10} {'Time':>10} {'Speedup':>10}")
    print(f"    {'─'*20} {'─'*6} {'─'*10} {'─'*10} {'─'*10}")
    
    color = G if speedup >= 2 else (C if speedup >= 1 else R)
    print(f"    {'Trigram index':<20} {len(indexed):>6} {indexed.scanned:>10} {fmt_us(indexed.elapsed_us):>10} {color}{B}{speedup:.1f}x faster{Z}")
    print(f"    {'Linear scan':<20} {len(linear):>6} {linear.scanned:>10} {fmt_us(linear.elapsed_us):>10} {'baseline':>10}")
    
    if indexed.trigrams_used:
        tris = ", ".join(sorted(list(indexed.trigrams_used)[:8]))
        if len(indexed.trigrams_used) > 8:
            tris += f" ... (+{len(indexed.trigrams_used) - 8} more)"
        print(f"    {D}trigrams: {tris}{Z}")
        print(f"    {D}selectivity: {indexed.selectivity:.1%} ({indexed.candidates}/{indexed.total_docs} candidates){Z}")
    
    return speedup


def main():
    ap = argparse.ArgumentParser(description="TurboDB Trigram Search Demo")
    ap.add_argument("--docs", type=int, default=50_000, help="number of documents (default 50K)")
    args = ap.parse_args()
    
    n_docs = args.docs

    print(f"\n{B}{'=' * 70}{Z}")
    print(f"{B}  TurboDB Trigram Search Index Demo{Z}")
    print(f"{B}  Cursor-style indexed regex search over document databases{Z}")
    print(f"{B}{'=' * 70}{Z}")

    # ── Generate documents ────────────────────────────────────────────────────
    print(f"\n{D}Generating {fmt(n_docs)} documents (code snippets, logs, configs)...{Z}", end=" ", flush=True)
    t0 = time.perf_counter()
    docs = [generate_doc(i) for i in range(n_docs)]
    gen_time = time.perf_counter() - t0
    print(f"{G}done{Z} ({gen_time:.1f}s)")

    total_bytes = sum(len(v) for _, v in docs)
    print(f"  {fmt(n_docs)} docs, {total_bytes / 1e6:.1f} MB total text")

    # ── Build trigram index ───────────────────────────────────────────────────
    print(f"\n{D}Building trigram index...{Z}", end=" ", flush=True)
    idx = TrigramIndex()
    idx.index_bulk("docs", docs)
    print(f"{G}done{Z}")
    print(f"  {fmt(idx.stats['trigrams'])} unique trigrams")
    print(f"  index time: {idx.stats['index_time_ms']:.0f}ms ({n_docs / (idx.stats['index_time_ms'] / 1000):.0f} docs/sec)")

    # ── Run searches ──────────────────────────────────────────────────────────
    print(f"\n{B}{'=' * 70}{Z}")
    print(f"{B}  Search Benchmarks: Trigram Index vs Linear Scan{Z}")
    print(f"{B}{'=' * 70}{Z}")

    speedups = []

    # Substring searches
    speedups.append(run_comparison(idx, "Exact function name", "handleRequest"))
    speedups.append(run_comparison(idx, "Error message", "connection refused"))
    speedups.append(run_comparison(idx, "Config key", "CONNECTION_POOL_SIZE"))
    speedups.append(run_comparison(idx, "URL pattern", "api.example.com"))
    speedups.append(run_comparison(idx, "Import statement", 'const std = @import("std")'))
    speedups.append(run_comparison(idx, "Rare string", "deadlock detected"))

    # Regex searches
    speedups.append(run_comparison(idx, "Regex: error codes", r"status=[45]\d\d", is_regex=True))
    speedups.append(run_comparison(idx, "Regex: function def", r"def \w+\(.*timeout", is_regex=True))
    speedups.append(run_comparison(idx, "Regex: HTTP latency", r"latency=\d{4}ms", is_regex=True))
    speedups.append(run_comparison(idx, "Regex: IP-like", r"\d+\.\d+\.\d+\.\d+", is_regex=True))
    speedups.append(run_comparison(idx, "Regex: Bearer token", r"Bearer \$\{", is_regex=True))
    speedups.append(run_comparison(idx, "Regex: Rust error", r"anyhow::bail!", is_regex=True))

    # ── Summary ───────────────────────────────────────────────────────────────
    avg_speedup = sum(speedups) / len(speedups)
    max_speedup = max(speedups)
    
    print(f"\n{B}{'=' * 70}{Z}")
    print(f"{B}  Summary{Z}")
    print(f"{B}{'=' * 70}{Z}")
    print(f"  Documents:        {fmt(n_docs)}")
    print(f"  Corpus size:      {total_bytes / 1e6:.1f} MB")
    print(f"  Unique trigrams:  {fmt(idx.stats['trigrams'])}")
    print(f"  Index build time: {idx.stats['index_time_ms']:.0f}ms")
    print()
    print(f"  {G}{B}Average speedup: {avg_speedup:.1f}x faster than linear scan{Z}")
    print(f"  {G}{B}Best speedup:    {max_speedup:.1f}x faster{Z}")
    print()
    print(f"  {D}Trigram indexing narrows the search space before full matching,{Z}")
    print(f"  {D}just like Cursor's approach for fast regex search in large codebases.{Z}")
    print(f"  {D}The rarer the search term, the higher the speedup.{Z}")
    print()


if __name__ == "__main__":
    main()
