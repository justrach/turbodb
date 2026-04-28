# Apple Container Benchmark Matrix

This harness compares TurboDB against PostgreSQL 18, MySQL, and optionally
TigerBeetle using the same generated application shape. TurboDB is measured two
ways: over HTTP and through the embedded C ABI/FFI bridge.

It must be run through Apple `container`. The runner creates a private container
network, starts each database in a container, runs the Python benchmark client in
another container, and does not publish database ports to macOS.

## Workload Shape

- `users`: keyed user records with an email and JSON profile.
- `orders`: keyed order records with `user_id`, amount, status, and JSON payload.
- TurboDB stores `bench_users`, `bench_orders`, and a materialized
  `bench_user_orders` edge collection. The benchmark client uses persistent
  HTTP plus `POST /db/:col/batch_get` for relationship fetches, so it does not
  inflate TurboDB latency with one TCP/HTTP setup per document.
- `turbodb_ffi` loads `/work/zig-out-ffi/lib/libturbodb.so` with Python `ctypes`.
  This is an embedded/raw-engine path, not a network database path, and exists
  to show the cost of HTTP/client transport separately from storage operations.
- PostgreSQL 18 and MySQL use normalized `users` and `orders` tables with a
  secondary index on `orders.user_id`.
- TigerBeetle uses accounts and transfers, so relationship lookups are modeled
  as account transfer queries rather than SQL/document joins. Updates and
  deletes are reported as not applicable because transfers are immutable.

## Workloads

- `ingest`: create all users and orders.
- `point_get`: read users by primary key.
- `relationship_lookup`: fetch all orders/transfers for a user.
- `join_or_join_like`: SQL join for PostgreSQL/MySQL, materialized edge fetches
  for TurboDB, account transfer query for TigerBeetle.
- `update_orders`: update order status where the engine supports mutation.
- `delete_orders`: delete orders where the engine supports deletion.

## Run

Small smoke run:

```sh
python3 bench/run_apple_container_bench.py \
  --users 50 \
  --orders-per-user 3 \
  --samples 25 \
  --skip-tigerbeetle
```

Larger run with all engines:

```sh
python3 bench/run_apple_container_bench.py \
  --users 10000 \
  --orders-per-user 5 \
  --samples 2000
```

Useful flags:

- `--skip-turbodb`, `--skip-postgres`, `--skip-mysql`, `--skip-tigerbeetle`
- `--skip-turbodb-ffi`
- `--turbodb-ffi-target aarch64-linux-gnu`
- `--turbodb-ffi-prefix zig-out-ffi`
- `--turbodb-ffi-lib /work/zig-out-ffi/lib/libturbodb.so`
- `--mysql-image mysql:8.4`
- `--postgres-image postgres:18`
- `--tigerbeetle-image ghcr.io/tigerbeetle/tigerbeetle:latest`
- `--tigerbeetle-memory 2G` because the replica journal needs more than the
  Apple container 1 GiB default in practice.
- `--keep` to leave containers, network, and volumes behind for inspection.

Results are written under ignored `benchmark-results/`.

PostgreSQL 18 is mounted at `/var/lib/postgresql`, not
`/var/lib/postgresql/data`, because the official image stores data in
versioned subdirectories for upgrade compatibility.
