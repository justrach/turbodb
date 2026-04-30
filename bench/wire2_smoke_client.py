#!/usr/bin/env python3
"""Smoke and throughput probe for TurboDB wire2."""

from __future__ import annotations

import argparse
import json
import socket
import struct
import time
from dataclasses import dataclass
from pathlib import Path


OUTER_OP_WIRE2 = 0x20
WIRE2_VERSION = 2

OP_HELLO = 0x00
OP_PING = 0x01
OP_GET = 0x02
OP_PUT = 0x03
OP_INSERT = 0x04
OP_DELETE = 0x05
OP_BATCH_GET = 0x06
OP_BULK_INSERT = 0x07
OP_BULK_UPSERT = 0x08
OP_BULK_DELETE = 0x09
OP_AUTH = 0x0A

STATUS_OK = 0x00
STATUS_NOT_FOUND = 0x01


@dataclass
class Response:
    request_id: int
    op: int
    status: int
    body: bytes


class Wire2Client:
    def __init__(self, host: str, port: int, timeout: float) -> None:
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.request_id = 1

    def close(self) -> None:
        self.sock.close()

    def request(self, op: int, body: bytes = b"") -> Response:
        rid = self.request_id
        self.request_id += 1
        payload = struct.pack("<BBQBBI", WIRE2_VERSION, 0, rid, op, 0, len(body)) + body
        frame = struct.pack(">IB", len(payload) + 5, OUTER_OP_WIRE2) + payload
        self.sock.sendall(frame)
        hdr = self._read_exact(5)
        frame_len, outer_op = struct.unpack(">IB", hdr)
        if outer_op != OUTER_OP_WIRE2 or frame_len < 21:
            raise RuntimeError(f"bad outer response op={outer_op} len={frame_len}")
        payload = self._read_exact(frame_len - 5)
        version, _flags, resp_id, resp_op, status, body_len = struct.unpack("<BBQBBI", payload[:16])
        if version != WIRE2_VERSION:
            raise RuntimeError(f"bad wire2 version {version}")
        if resp_id != rid or resp_op != op:
            raise RuntimeError(f"response mismatch rid={resp_id}/{rid} op={resp_op}/{op}")
        body = payload[16:]
        if len(body) != body_len:
            raise RuntimeError(f"body length mismatch got={len(body)} want={body_len}")
        return Response(resp_id, resp_op, status, body)

    def _read_exact(self, n: int) -> bytes:
        chunks: list[bytes] = []
        remaining = n
        while remaining:
            chunk = self.sock.recv(remaining)
            if not chunk:
                raise RuntimeError("connection closed")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)


def key_payload(collection: str, key: str) -> bytes:
    col = collection.encode()
    k = key.encode()
    return struct.pack("<H", len(col)) + col + struct.pack("<H", len(k)) + k


def kv_payload(collection: str, key: str, value: bytes) -> bytes:
    col = collection.encode()
    k = key.encode()
    return struct.pack("<H", len(col)) + col + struct.pack("<H", len(k)) + k + struct.pack("<I", len(value)) + value


def collection_batch_prefix(collection: str, count: int) -> bytes:
    col = collection.encode()
    return struct.pack("<H", len(col)) + col + struct.pack("<I", count)


def key_record(key: str) -> bytes:
    k = key.encode()
    return struct.pack("<H", len(k)) + k


def kv_record(key: str, value: bytes) -> bytes:
    k = key.encode()
    return struct.pack("<HI", len(k), len(value)) + k + value


def decode_stats(body: bytes) -> dict[str, int]:
    if len(body) != 28:
        raise RuntimeError(f"bad stats body length {len(body)}")
    inserted, updated, deleted, missing, errors, byte_count = struct.unpack("<IIIIIQ", body)
    return {
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "missing": missing,
        "errors": errors,
        "bytes": byte_count,
    }


def decode_get(body: bytes) -> bytes:
    if len(body) < 13:
        raise RuntimeError("bad get body")
    value_len = struct.unpack("<I", body[9:13])[0]
    value = body[13 : 13 + value_len]
    if len(value) != value_len:
        raise RuntimeError("truncated get value")
    return value


def decode_batch_get(body: bytes) -> list[tuple[int, bytes]]:
    if len(body) < 4:
        raise RuntimeError("bad batch get body")
    count = struct.unpack("<I", body[:4])[0]
    pos = 4
    out: list[tuple[int, bytes]] = []
    for _ in range(count):
        if pos + 14 > len(body):
            raise RuntimeError("truncated batch item")
        status = body[pos]
        value_len = struct.unpack("<I", body[pos + 10 : pos + 14])[0]
        pos += 14
        value = body[pos : pos + value_len]
        if len(value) != value_len:
            raise RuntimeError("truncated batch value")
        pos += value_len
        out.append((status, value))
    if pos != len(body):
        raise RuntimeError("trailing batch bytes")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="TurboDB wire2 smoke client")
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=27017)
    ap.add_argument("--collection", default="wire2_events")
    ap.add_argument("--ops", type=int, default=1000)
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--get-rounds", type=int, default=1)
    ap.add_argument("--output")
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--auth-key")
    args = ap.parse_args()

    client = Wire2Client(args.host, args.port, args.timeout)
    try:
        hello = client.request(OP_HELLO)
        if hello.status != STATUS_OK:
            raise RuntimeError(f"hello failed status={hello.status}")
        max_frame, max_response, features = struct.unpack("<III", hello.body)

        if args.auth_key:
            key = args.auth_key.encode()
            auth_resp = client.request(OP_AUTH, struct.pack("<H", len(key)) + key)
            if auth_resp.status != STATUS_OK:
                raise RuntimeError(f"auth failed status={auth_resp.status}")

        one_value = json.dumps({"kind": "single", "n": 1}, separators=(",", ":")).encode()
        put = client.request(OP_PUT, kv_payload(args.collection, "single", one_value))
        if put.status != STATUS_OK:
            raise RuntimeError(f"put failed status={put.status}")

        got = client.request(OP_GET, key_payload(args.collection, "single"))
        if got.status != STATUS_OK or decode_get(got.body) != one_value:
            raise RuntimeError(f"get failed status={got.status}")

        missing = client.request(OP_GET, key_payload(args.collection, "missing"))
        if missing.status != STATUS_NOT_FOUND:
            raise RuntimeError(f"missing get expected not_found got={missing.status}")

        remaining = args.ops
        key_index = 0
        bulk_seconds = 0.0
        inserted = 0
        while remaining > 0:
            n = min(args.batch_size, remaining)
            body = bytearray(collection_batch_prefix(args.collection, n))
            for _ in range(n):
                value = json.dumps({"kind": "bulk", "n": key_index}, separators=(",", ":")).encode()
                body += kv_record(f"k-{key_index:08d}", value)
                key_index += 1
            start = time.perf_counter()
            resp = client.request(OP_BULK_INSERT, bytes(body))
            bulk_seconds += time.perf_counter() - start
            if resp.status != STATUS_OK:
                raise RuntimeError(f"bulk insert failed status={resp.status}")
            stats = decode_stats(resp.body)
            if stats["errors"] != 0:
                raise RuntimeError(f"bulk insert errors: {stats}")
            inserted += stats["inserted"]
            remaining -= n

        get_batch_size = min(args.batch_size, args.ops)
        batch_seconds = 0.0
        batch_items = 0
        found = 0
        not_found = 0
        for round_idx in range(args.get_rounds):
            body = bytearray(collection_batch_prefix(args.collection, get_batch_size + 1))
            offset = (round_idx * get_batch_size) % max(args.ops, 1)
            for i in range(get_batch_size):
                body += key_record(f"k-{(offset + i) % args.ops:08d}")
            body += key_record(f"does-not-exist-{round_idx}")
            start = time.perf_counter()
            batch_resp = client.request(OP_BATCH_GET, bytes(body))
            batch_seconds += time.perf_counter() - start
            if batch_resp.status != STATUS_OK:
                raise RuntimeError(f"batch get failed status={batch_resp.status}")
            items = decode_batch_get(batch_resp.body)
            batch_items += len(items)
            found += sum(1 for status, _ in items if status == STATUS_OK)
            not_found += sum(1 for status, _ in items if status == STATUS_NOT_FOUND)

        summary = {
            "status": "ok",
            "config": {
                "ops": args.ops,
                "batch_size": args.batch_size,
                "get_rounds": args.get_rounds,
                "collection": args.collection,
                "auth": bool(args.auth_key),
            },
            "hello": {
                "max_frame": max_frame,
                "max_response": max_response,
                "features": features,
            },
            "single_put": decode_stats(put.body),
            "bulk_insert": {
                "inserted": inserted,
                "seconds": bulk_seconds,
                "ops_sec": inserted / bulk_seconds if bulk_seconds > 0 else None,
            },
            "batch_get": {
                "items": batch_items,
                "found": found,
                "not_found": not_found,
                "seconds": batch_seconds,
                "ops_sec": batch_items / batch_seconds if batch_seconds > 0 else None,
            },
        }
        if args.output:
            out_path = Path(args.output)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
