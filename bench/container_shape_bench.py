#!/usr/bin/env python3
"""
Container runner for keyed document / relational / transfer-shaped benchmarks.

This script is intended to run inside an Apple container network created by
run_apple_container_bench.py. It connects only to private container IPs.
"""
from __future__ import annotations

import argparse
import json
import math
import random
import statistics
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable


def now() -> float:
    return time.perf_counter()


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * (p / 100.0)
    lo = int(math.floor(k))
    hi = min(lo + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def metric(elapsed: float, ops: int, latencies: list[float] | None = None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    latencies = latencies or []
    result = {
        "ops": ops,
        "seconds": elapsed,
        "ops_sec": ops / elapsed if elapsed > 0 else 0,
    }
    if latencies:
        result.update({
            "p50_ms": percentile(latencies, 50),
            "p95_ms": percentile(latencies, 95),
            "p99_ms": percentile(latencies, 99),
        })
    if extra:
        result.update(extra)
    return result


def timed(fn: Callable[[], Any], ops: int, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    start = now()
    fn()
    return metric(now() - start, ops, extra=extra)


def timed_loop(items: list[Any], fn: Callable[[Any], Any], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    latencies: list[float] = []
    start = now()
    for item in items:
        op_start = now()
        fn(item)
        latencies.append((now() - op_start) * 1000.0)
    return metric(now() - start, len(items), latencies, extra)


def user_doc(user_id: int) -> dict[str, Any]:
    return {
        "id": user_id,
        "email": f"user{user_id}@example.test",
        "profile": {
            "tier": user_id % 7,
            "region": f"r{user_id % 16}",
            "active": True,
        },
    }


def order_doc(order_id: int, user_id: int) -> dict[str, Any]:
    return {
        "id": order_id,
        "user_id": user_id,
        "amount": 100 + (order_id % 10_000),
        "status": "open" if order_id % 3 else "settled",
        "payload": {
            "sku": f"sku-{order_id % 97}",
            "memo": f"order memo {order_id}",
        },
    }


@dataclass
class Shape:
    users: int
    orders_per_user: int

    @property
    def orders(self) -> int:
        return self.users * self.orders_per_user

    def user_ids(self) -> range:
        return range(1, self.users + 1)

    def order_id(self, user_id: int, ordinal: int) -> int:
        return (user_id - 1) * self.orders_per_user + ordinal + 1

    def order_ids_for_user(self, user_id: int) -> list[int]:
        return [self.order_id(user_id, i) for i in range(self.orders_per_user)]


class TurboDBBench:
    name = "turbodb"

    def __init__(self, host: str, port: int, shape: Shape, batch_size: int):
        self.base = f"http://{host}:{port}"
        self.shape = shape
        self.batch_size = batch_size

    def request(self, method: str, path: str, body: bytes | None = None) -> bytes:
        req = urllib.request.Request(
            self.base + path,
            data=body,
            method=method,
            headers={"content-type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read()

    def wait(self) -> None:
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                self.request("GET", "/health")
                return
            except Exception:
                time.sleep(0.25)
        raise RuntimeError("TurboDB did not become healthy")

    def drop(self) -> None:
        for col in ("bench_users", "bench_orders", "bench_user_orders"):
            try:
                self.request("DELETE", f"/db/{col}")
            except Exception:
                pass

    def bulk(self, collection: str, rows: list[tuple[str, dict[str, Any]]]) -> None:
        lines = []
        for key, value in rows:
            lines.append(json.dumps({"key": key, "value": value}, separators=(",", ":")))
        self.request("POST", f"/db/{collection}/bulk", ("\n".join(lines) + "\n").encode())

    def ingest(self) -> dict[str, Any]:
        self.drop()

        def run() -> None:
            rows: list[tuple[str, dict[str, Any]]] = []
            for uid in self.shape.user_ids():
                rows.append((str(uid), user_doc(uid)))
                if len(rows) >= self.batch_size:
                    self.bulk("bench_users", rows)
                    rows.clear()
            if rows:
                self.bulk("bench_users", rows)

            rows.clear()
            for uid in self.shape.user_ids():
                for oid in self.shape.order_ids_for_user(uid):
                    rows.append((str(oid), order_doc(oid, uid)))
                    if len(rows) >= self.batch_size:
                        self.bulk("bench_orders", rows)
                        rows.clear()
            if rows:
                self.bulk("bench_orders", rows)

            rows.clear()
            for uid in self.shape.user_ids():
                rows.append((str(uid), {"user_id": uid, "order_ids": self.shape.order_ids_for_user(uid)}))
                if len(rows) >= self.batch_size:
                    self.bulk("bench_user_orders", rows)
                    rows.clear()
            if rows:
                self.bulk("bench_user_orders", rows)

        logical = self.shape.users + self.shape.orders
        physical = logical + self.shape.users
        return timed(run, logical, {"physical_writes": physical})

    def get_doc(self, collection: str, key: int) -> dict[str, Any]:
        return json.loads(self.request("GET", f"/db/{collection}/{key}").decode())

    def point_get(self, samples: list[int]) -> dict[str, Any]:
        return timed_loop(samples, lambda uid: self.get_doc("bench_users", uid))

    def order_lookup(self, samples: list[int]) -> dict[str, Any]:
        def op(uid: int) -> None:
            edge = self.get_doc("bench_user_orders", uid)["value"]
            for oid in edge["order_ids"]:
                self.get_doc("bench_orders", oid)

        return timed_loop(samples, op, {"orders_per_lookup": self.shape.orders_per_user})

    def join_like(self, samples: list[int]) -> dict[str, Any]:
        def op(uid: int) -> None:
            user = self.get_doc("bench_users", uid)["value"]
            edge = self.get_doc("bench_user_orders", uid)["value"]
            orders = [self.get_doc("bench_orders", oid)["value"] for oid in edge["order_ids"]]
            _ = (user["email"], len(orders))

        return timed_loop(samples, op, {"mode": "materialized_edge_point_gets"})

    def update_orders(self, samples: list[int]) -> dict[str, Any]:
        def op(oid: int) -> None:
            value = order_doc(oid, ((oid - 1) // self.shape.orders_per_user) + 1)
            value["status"] = "updated"
            self.request("PUT", f"/db/bench_orders/{oid}", json.dumps(value).encode())

        return timed_loop(samples, op)

    def delete_orders(self, samples: list[int]) -> dict[str, Any]:
        return timed_loop(samples, lambda oid: self.request("DELETE", f"/db/bench_orders/{oid}"))


class PostgresBench:
    name = "postgresql18"

    def __init__(self, host: str, shape: Shape, batch_size: int):
        import psycopg

        self.conn = psycopg.connect(host=host, port=5432, dbname="bench", user="postgres", password="postgres", autocommit=True)
        self.shape = shape
        self.batch_size = batch_size

    def wait(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("select 1")

    def drop(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("drop table if exists orders")
            cur.execute("drop table if exists users")
            cur.execute("create table users (id bigint primary key, email text not null, profile jsonb not null)")
            cur.execute("create table orders (id bigint primary key, user_id bigint not null references users(id), amount bigint not null, status text not null, payload jsonb not null)")
            cur.execute("create index orders_user_id_idx on orders(user_id)")

    def ingest(self) -> dict[str, Any]:
        self.drop()

        def run() -> None:
            with self.conn.cursor() as cur:
                cur.executemany(
                    "insert into users (id,email,profile) values (%s,%s,%s::jsonb)",
                    [(uid, user_doc(uid)["email"], json.dumps(user_doc(uid)["profile"])) for uid in self.shape.user_ids()],
                )
                rows = []
                for uid in self.shape.user_ids():
                    for oid in self.shape.order_ids_for_user(uid):
                        doc = order_doc(oid, uid)
                        rows.append((oid, uid, doc["amount"], doc["status"], json.dumps(doc["payload"])))
                cur.executemany(
                    "insert into orders (id,user_id,amount,status,payload) values (%s,%s,%s,%s,%s::jsonb)",
                    rows,
                )

        return timed(run, self.shape.users + self.shape.orders)

    def point_get(self, samples: list[int]) -> dict[str, Any]:
        def op(uid: int) -> None:
            with self.conn.cursor() as cur:
                cur.execute("select id,email,profile from users where id=%s", (uid,))
                cur.fetchone()

        return timed_loop(samples, op)

    def order_lookup(self, samples: list[int]) -> dict[str, Any]:
        def op(uid: int) -> None:
            with self.conn.cursor() as cur:
                cur.execute("select id,amount,status,payload from orders where user_id=%s", (uid,))
                cur.fetchall()

        return timed_loop(samples, op, {"orders_per_lookup": self.shape.orders_per_user})

    def join_like(self, samples: list[int]) -> dict[str, Any]:
        def op(uid: int) -> None:
            with self.conn.cursor() as cur:
                cur.execute(
                    "select u.email,o.id,o.amount,o.payload from users u join orders o on o.user_id=u.id where u.id=%s",
                    (uid,),
                )
                cur.fetchall()

        return timed_loop(samples, op, {"mode": "sql_join"})

    def update_orders(self, samples: list[int]) -> dict[str, Any]:
        def op(oid: int) -> None:
            with self.conn.cursor() as cur:
                cur.execute("update orders set status='updated' where id=%s", (oid,))

        return timed_loop(samples, op)

    def delete_orders(self, samples: list[int]) -> dict[str, Any]:
        def op(oid: int) -> None:
            with self.conn.cursor() as cur:
                cur.execute("delete from orders where id=%s", (oid,))

        return timed_loop(samples, op)


class MySQLBench:
    name = "mysql"

    def __init__(self, host: str, shape: Shape, batch_size: int):
        import pymysql

        self.conn = pymysql.connect(host=host, port=3306, user="root", password="mysql", database="bench", autocommit=True)
        self.shape = shape
        self.batch_size = batch_size

    def wait(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("select 1")

    def drop(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("drop table if exists orders")
            cur.execute("drop table if exists users")
            cur.execute("create table users (id bigint primary key, email varchar(255) not null, profile json not null)")
            cur.execute("create table orders (id bigint primary key, user_id bigint not null, amount bigint not null, status varchar(32) not null, payload json not null, index orders_user_id_idx(user_id))")

    def ingest(self) -> dict[str, Any]:
        self.drop()

        def run() -> None:
            with self.conn.cursor() as cur:
                cur.executemany(
                    "insert into users (id,email,profile) values (%s,%s,%s)",
                    [(uid, user_doc(uid)["email"], json.dumps(user_doc(uid)["profile"])) for uid in self.shape.user_ids()],
                )
                rows = []
                for uid in self.shape.user_ids():
                    for oid in self.shape.order_ids_for_user(uid):
                        doc = order_doc(oid, uid)
                        rows.append((oid, uid, doc["amount"], doc["status"], json.dumps(doc["payload"])))
                cur.executemany(
                    "insert into orders (id,user_id,amount,status,payload) values (%s,%s,%s,%s,%s)",
                    rows,
                )

        return timed(run, self.shape.users + self.shape.orders)

    def point_get(self, samples: list[int]) -> dict[str, Any]:
        def op(uid: int) -> None:
            with self.conn.cursor() as cur:
                cur.execute("select id,email,profile from users where id=%s", (uid,))
                cur.fetchone()

        return timed_loop(samples, op)

    def order_lookup(self, samples: list[int]) -> dict[str, Any]:
        def op(uid: int) -> None:
            with self.conn.cursor() as cur:
                cur.execute("select id,amount,status,payload from orders where user_id=%s", (uid,))
                cur.fetchall()

        return timed_loop(samples, op, {"orders_per_lookup": self.shape.orders_per_user})

    def join_like(self, samples: list[int]) -> dict[str, Any]:
        def op(uid: int) -> None:
            with self.conn.cursor() as cur:
                cur.execute(
                    "select u.email,o.id,o.amount,o.payload from users u join orders o on o.user_id=u.id where u.id=%s",
                    (uid,),
                )
                cur.fetchall()

        return timed_loop(samples, op, {"mode": "sql_join"})

    def update_orders(self, samples: list[int]) -> dict[str, Any]:
        def op(oid: int) -> None:
            with self.conn.cursor() as cur:
                cur.execute("update orders set status='updated' where id=%s", (oid,))

        return timed_loop(samples, op)

    def delete_orders(self, samples: list[int]) -> dict[str, Any]:
        def op(oid: int) -> None:
            with self.conn.cursor() as cur:
                cur.execute("delete from orders where id=%s", (oid,))

        return timed_loop(samples, op)


class TigerBeetleBench:
    name = "tigerbeetle"

    def __init__(self, host: str, shape: Shape, batch_size: int):
        import tigerbeetle as tb

        self.tb = tb
        self.client = tb.ClientSync(cluster_id=0, replica_addresses=f"{host}:3000")
        self.shape = shape
        # Keep TigerBeetle client batches conservative; the wire limit is based
        # on encoded message size, not just object count.
        self.batch_size = min(batch_size, 128)
        self.merchant_id = 9_000_000_000_000

    def wait(self) -> None:
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                self.client.lookup_accounts([1])
                return
            except Exception:
                time.sleep(0.25)
        raise RuntimeError("TigerBeetle did not become ready")

    def batches(self, values: list[Any]) -> list[list[Any]]:
        return [values[i:i + self.batch_size] for i in range(0, len(values), self.batch_size)]

    def ingest(self) -> dict[str, Any]:
        tb = self.tb

        def run() -> None:
            accounts = [
                tb.Account(
                    id=uid,
                    debits_pending=0,
                    debits_posted=0,
                    credits_pending=0,
                    credits_posted=0,
                    user_data_128=uid,
                    user_data_64=0,
                    user_data_32=0,
                    ledger=1,
                    code=100,
                    flags=0,
                    timestamp=0,
                )
                for uid in self.shape.user_ids()
            ]
            accounts.append(tb.Account(
                id=self.merchant_id,
                debits_pending=0,
                debits_posted=0,
                credits_pending=0,
                credits_posted=0,
                user_data_128=0,
                user_data_64=0,
                user_data_32=0,
                ledger=1,
                code=200,
                flags=0,
                timestamp=0,
            ))
            for batch in self.batches(accounts):
                self.client.create_accounts(batch)

            transfers = []
            for uid in self.shape.user_ids():
                for oid in self.shape.order_ids_for_user(uid):
                    transfers.append(tb.Transfer(
                        id=oid,
                        debit_account_id=uid,
                        credit_account_id=self.merchant_id,
                        amount=100 + (oid % 10_000),
                        pending_id=0,
                        user_data_128=uid,
                        user_data_64=oid,
                        user_data_32=0,
                        timeout=0,
                        ledger=1,
                        code=300,
                        flags=0,
                        timestamp=0,
                    ))
            for batch in self.batches(transfers):
                self.client.create_transfers(batch)

        return timed(run, self.shape.users + self.shape.orders, {"model": "accounts_and_transfers"})

    def point_get(self, samples: list[int]) -> dict[str, Any]:
        def op(uid: int) -> None:
            self.client.lookup_accounts([uid])

        return timed_loop(samples, op)

    def order_lookup(self, samples: list[int]) -> dict[str, Any]:
        tb = self.tb
        flags = tb.AccountFilterFlags.DEBITS | tb.AccountFilterFlags.CREDITS

        def op(uid: int) -> None:
            account_filter = tb.AccountFilter(
                account_id=uid,
                user_data_128=0,
                user_data_64=0,
                user_data_32=0,
                code=0,
                timestamp_min=0,
                timestamp_max=0,
                limit=max(self.shape.orders_per_user, 1),
                flags=flags,
            )
            self.client.get_account_transfers(account_filter)

        return timed_loop(samples, op, {"orders_per_lookup": self.shape.orders_per_user, "model": "get_account_transfers"})

    def join_like(self, samples: list[int]) -> dict[str, Any]:
        result = self.order_lookup(samples)
        result["mode"] = "account_transfer_query_not_sql_join"
        return result

    def update_orders(self, samples: list[int]) -> dict[str, Any]:
        return {"not_applicable": True, "reason": "TigerBeetle accounts/transfers are immutable after creation"}

    def delete_orders(self, samples: list[int]) -> dict[str, Any]:
        return {"not_applicable": True, "reason": "TigerBeetle does not delete accounts/transfers"}


def run_engine(engine: Any, shape: Shape, samples: int) -> dict[str, Any]:
    rng = random.Random(42)
    user_samples = [rng.randint(1, shape.users) for _ in range(samples)]
    order_samples = [rng.randint(1, shape.orders) for _ in range(samples)]
    delete_order_samples = rng.sample(range(1, shape.orders + 1), min(samples, shape.orders))
    return {
        "ingest": engine.ingest(),
        "point_get": engine.point_get(user_samples),
        "relationship_lookup": engine.order_lookup(user_samples),
        "join_or_join_like": engine.join_like(user_samples),
        "update_orders": engine.update_orders(order_samples),
        "delete_orders": engine.delete_orders(delete_order_samples),
    }


def print_summary(results: dict[str, Any]) -> None:
    workloads = [
        "ingest",
        "point_get",
        "relationship_lookup",
        "join_or_join_like",
        "update_orders",
        "delete_orders",
    ]
    print("\nContainer Shape Benchmark")
    print("=" * 96)
    print(f"{'workload':<24} {'engine':<14} {'ops/sec':>12} {'p50 ms':>10} {'p95 ms':>10} {'notes'}")
    print("-" * 96)
    for workload in workloads:
        for engine, engine_results in results["engines"].items():
            row = engine_results.get(workload)
            if not row:
                continue
            if row.get("not_applicable"):
                print(f"{workload:<24} {engine:<14} {'n/a':>12} {'':>10} {'':>10} {row.get('reason', '')}")
                continue
            notes = row.get("mode") or row.get("model") or ""
            print(
                f"{workload:<24} {engine:<14} {row.get('ops_sec', 0):>12.0f} "
                f"{row.get('p50_ms', 0):>10.3f} {row.get('p95_ms', 0):>10.3f} {notes}"
            )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, default=1000)
    ap.add_argument("--orders-per-user", type=int, default=3)
    ap.add_argument("--samples", type=int, default=500)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--output", default="/work/benchmark-results/container-shape-bench.json")
    ap.add_argument("--turbodb-host")
    ap.add_argument("--turbodb-port", type=int, default=27017)
    ap.add_argument("--postgres-host")
    ap.add_argument("--mysql-host")
    ap.add_argument("--tigerbeetle-host")
    args = ap.parse_args()
    if args.users < 1:
        raise SystemExit("--users must be at least 1")
    if args.orders_per_user < 1:
        raise SystemExit("--orders-per-user must be at least 1")
    if args.samples < 1:
        raise SystemExit("--samples must be at least 1")

    shape = Shape(args.users, args.orders_per_user)
    results: dict[str, Any] = {
        "config": {
            "users": shape.users,
            "orders": shape.orders,
            "orders_per_user": shape.orders_per_user,
            "samples": args.samples,
            "batch_size": args.batch_size,
        },
        "engines": {},
        "errors": {},
    }

    engines: list[Any] = []
    if args.turbodb_host:
        engines.append(TurboDBBench(args.turbodb_host, args.turbodb_port, shape, args.batch_size))
    if args.postgres_host:
        engines.append(PostgresBench(args.postgres_host, shape, args.batch_size))
    if args.mysql_host:
        engines.append(MySQLBench(args.mysql_host, shape, args.batch_size))
    if args.tigerbeetle_host:
        engines.append(TigerBeetleBench(args.tigerbeetle_host, shape, args.batch_size))

    for engine in engines:
        try:
            engine.wait()
            results["engines"][engine.name] = run_engine(engine, shape, min(args.samples, shape.users, max(shape.orders, 1)))
        except Exception as exc:
            results["errors"][engine.name] = repr(exc)
            print(f"{engine.name} failed: {exc}", file=sys.stderr)

    print_summary(results)

    import os
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"\nwrote {args.output}")
    return 0 if results["engines"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
