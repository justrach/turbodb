#!/usr/bin/env python3
"""Concurrent agent-shaped client workload for the nanoapi TurboDB proxy."""

from __future__ import annotations

import argparse
import concurrent.futures
import http.client
import json
import random
import statistics
import string
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class OpStats:
    latencies_ms: list[float] = field(default_factory=list)
    ok: int = 0
    errors: int = 0
    statuses: dict[int, int] = field(default_factory=dict)

    def add(self, latency_ms: float, status: int, ok: bool) -> None:
        self.latencies_ms.append(latency_ms)
        self.statuses[status] = self.statuses.get(status, 0) + 1
        if ok:
            self.ok += 1
        else:
            self.errors += 1


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Pound the nanoapi agent proxy through agent-shaped calls")
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=28080)
    ap.add_argument("--agents", type=int, default=32)
    ap.add_argument("--rounds", type=int, default=20)
    ap.add_argument("--events-per-round", type=int, default=3)
    ap.add_argument("--context-limit", type=int, default=20)
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--output", required=True)
    ap.add_argument("--run-id", default=f"agentic-nanoapi-{int(time.time())}")
    ap.add_argument("--seed", type=int, default=7)
    return ap.parse_args()


class AgentClient:
    def __init__(self, host: str, port: int, timeout: float):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.conn = http.client.HTTPConnection(host, port, timeout=timeout)

    def close(self) -> None:
        self.conn.close()

    def request(self, method: str, path: str, body: str = "") -> tuple[int, bytes]:
        headers = {"Content-Type": "application/json"}
        try:
            self.conn.request(method, path, body=body.encode("utf-8"), headers=headers)
            resp = self.conn.getresponse()
            data = resp.read()
            return resp.status, data
        except (http.client.HTTPException, OSError):
            self.conn.close()
            self.conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
            raise


def random_text(rng: random.Random, words: int = 24) -> str:
    parts: list[str] = []
    alphabet = string.ascii_lowercase
    for _ in range(words):
        parts.append("".join(rng.choice(alphabet) for _ in range(rng.randint(3, 10))))
    return " ".join(parts)


def timed(stats: dict[str, OpStats], name: str, fn) -> None:
    started = time.perf_counter()
    status = 0
    ok = False
    try:
        status, body = fn()
        ok = 200 <= status < 300 and body
    except Exception:
        status = 0
    finally:
        stats.setdefault(name, OpStats()).add((time.perf_counter() - started) * 1000.0, status, ok)


def run_agent(agent_id: int, args: argparse.Namespace) -> dict[str, OpStats]:
    rng = random.Random(args.seed + agent_id)
    client = AgentClient(args.host, args.port, args.timeout)
    stats: dict[str, OpStats] = {}
    try:
        for round_id in range(args.rounds):
            base_key = f"{args.run_id}-a{agent_id:03d}-r{round_id:03d}"
            message = {
                "run_id": args.run_id,
                "agent": agent_id,
                "round": round_id,
                "kind": "message",
                "thought_summary": random_text(rng, 12),
                "message": random_text(rng, 32),
                "tool_calls": [
                    {"name": "ziggrep", "args": {"pattern": "agent", "path": "MCP/harness"}},
                    {"name": "chat_post", "args": {"topic": "agentic-load", "role": f"agent-{agent_id:03d}"}},
                ],
            }

            timed(stats, "write_event", lambda m=message, k=base_key: client.request("POST", f"/agent/event/{k}", json.dumps(m, separators=(",", ":"))))

            tool = {
                "run_id": args.run_id,
                "agent": agent_id,
                "round": round_id,
                "kind": "tool_result",
                "status": "ok",
                "latency_ms": rng.randint(10, 400),
                "preview": random_text(rng, 18),
            }
            timed(stats, "write_tool", lambda t=tool, k=base_key: client.request("POST", f"/agent/tool/{k}", json.dumps(t, separators=(",", ":"))))

            batch_lines: list[str] = []
            for event_id in range(args.events_per_round):
                key = f"{base_key}-batch-{event_id:02d}"
                value = {
                    "run_id": args.run_id,
                    "agent": agent_id,
                    "round": round_id,
                    "event": event_id,
                    "kind": "trace",
                    "content": random_text(rng, 16),
                }
                batch_lines.append(json.dumps({"key": key, "value": value}, separators=(",", ":")))
            timed(stats, "batch_trace", lambda body="\n".join(batch_lines) + "\n": client.request("POST", "/agent/batch", body))

            if round_id > 0:
                prev_key = f"{args.run_id}-a{agent_id:03d}-r{round_id - 1:03d}"
                timed(stats, "read_previous", lambda k=prev_key: client.request("GET", f"/agent/event/{k}"))

            if round_id % 3 == 0:
                timed(stats, "context_scan", lambda: client.request("GET", f"/agent/context?limit={args.context_limit}"))
    finally:
        client.close()
    return stats


def merge_stats(results: list[dict[str, OpStats]]) -> dict[str, OpStats]:
    merged: dict[str, OpStats] = {}
    for result in results:
        for name, stats in result.items():
            dst = merged.setdefault(name, OpStats())
            dst.latencies_ms.extend(stats.latencies_ms)
            dst.ok += stats.ok
            dst.errors += stats.errors
            for status, count in stats.statuses.items():
                dst.statuses[status] = dst.statuses.get(status, 0) + count
    return merged


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int((len(ordered) - 1) * pct))
    return ordered[idx]


def summarize(stats: dict[str, OpStats], elapsed_s: float, args: argparse.Namespace, health: dict[str, Any]) -> dict[str, Any]:
    ops: dict[str, Any] = {}
    total_ok = 0
    total_errors = 0
    total_ops = 0
    for name, item in sorted(stats.items()):
        values = item.latencies_ms
        total_ok += item.ok
        total_errors += item.errors
        total_ops += len(values)
        ops[name] = {
            "ops": len(values),
            "ok": item.ok,
            "errors": item.errors,
            "statuses": item.statuses,
            "p50_ms": statistics.median(values) if values else 0.0,
            "p95_ms": percentile(values, 0.95),
            "p99_ms": percentile(values, 0.99),
        }
    return {
        "run_id": args.run_id,
        "config": {
            "agents": args.agents,
            "rounds": args.rounds,
            "events_per_round": args.events_per_round,
            "context_limit": args.context_limit,
        },
        "elapsed_s": elapsed_s,
        "ops_sec": total_ops / elapsed_s if elapsed_s > 0 else 0.0,
        "total_ops": total_ops,
        "ok": total_ok,
        "errors": total_errors,
        "operations": ops,
        "post_health": health,
    }


def gateway_health(host: str, port: int, timeout: float) -> dict[str, Any]:
    client = AgentClient(host, port, timeout)
    try:
        status, body = client.request("GET", "/health")
        turbo_status, turbo_body = client.request("GET", "/turbodb/health")
        return {
            "gateway_status": status,
            "gateway_body": body.decode("utf-8", "replace")[:500],
            "turbodb_status": turbo_status,
            "turbodb_body": turbo_body.decode("utf-8", "replace")[:500],
        }
    except Exception as exc:
        return {"error": repr(exc)}
    finally:
        client.close()


def main() -> int:
    args = parse_args()
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.agents) as pool:
        futures = [pool.submit(run_agent, i, args) for i in range(args.agents)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    elapsed = time.perf_counter() - started
    health = gateway_health(args.host, args.port, args.timeout)
    summary = summarize(merge_stats(results), elapsed, args, health)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, sort_keys=True)

    print(json.dumps({
        "run_id": args.run_id,
        "ops_sec": round(summary["ops_sec"], 2),
        "total_ops": summary["total_ops"],
        "errors": summary["errors"],
        "post_health": summary["post_health"],
    }, indent=2))
    return 0 if summary["errors"] == 0 and "error" not in summary["post_health"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
