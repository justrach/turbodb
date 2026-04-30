#!/usr/bin/env python3
"""
Run the document/relational/transfer benchmark matrix inside Apple containers.

The script keeps every database service on a private Apple container network and
runs the benchmark client from a Python container. It does not publish database
ports to the macOS host.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def shlex_join(argv: list[str]) -> str:
    import shlex

    return " ".join(shlex.quote(part) for part in argv)


def run(argv: list[str], *, check: bool = True, capture: bool = False, cwd: Path = ROOT, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
    print(f"+ {shlex_join(argv)}", flush=True)
    return subprocess.run(
        argv,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        check=check,
        timeout=timeout,
    )


def container_exists(name: str) -> bool:
    proc = run(["container", "inspect", name], check=False, capture=True)
    return proc.returncode == 0


def stop_container(name: str) -> None:
    if not container_exists(name):
        return
    run(["container", "stop", name], check=False, capture=True)


def delete_container(name: str) -> None:
    if not container_exists(name):
        return
    run(["container", "stop", name], check=False, capture=True)
    run(["container", "rm", name], check=False, capture=True)


def network_exists(name: str) -> bool:
    proc = run(["container", "network", "list"], check=False, capture=True)
    if proc.returncode != 0 or proc.stdout is None:
        return False
    return any(line.split(maxsplit=1)[0] == name for line in proc.stdout.splitlines()[1:])


def create_network(name: str) -> None:
    if not network_exists(name):
        run(["container", "network", "create", name])


def delete_network(name: str) -> None:
    if network_exists(name):
        run(["container", "network", "delete", name], check=False, capture=True)


def volume_exists(name: str) -> bool:
    proc = run(["container", "volume", "list"], check=False, capture=True)
    if proc.returncode != 0 or proc.stdout is None:
        return False
    return any(line.split(maxsplit=1)[0] == name for line in proc.stdout.splitlines()[1:])


def create_volume(name: str) -> None:
    if not volume_exists(name):
        run(["container", "volume", "create", name])


def delete_volume(name: str) -> None:
    if volume_exists(name):
        run(["container", "volume", "delete", name], check=False, capture=True)


def inspect_json(name: str) -> Any:
    proc = run(["container", "inspect", name], capture=True)
    assert proc.stdout is not None
    return json.loads(proc.stdout)


def container_ip(name: str) -> str:
    data = inspect_json(name)
    candidates: list[Any]
    if isinstance(data, list):
        candidates = data
    else:
        candidates = [data]

    for item in candidates:
        networks = item.get("networks") if isinstance(item, dict) else None
        if not networks:
            continue
        for net in networks:
            address = net.get("ipv4Address") or net.get("address")
            if address:
                return str(address).split("/", 1)[0]
    raise RuntimeError(f"could not find IPv4 address for container {name}")


def wait_http(network: str, url: str, timeout_s: int = 45) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        proc = run(
            ["container", "run", "--rm", "--network", network, "--entrypoint", "wget", "alpine:3.20", "-qO-", url],
            check=False,
            capture=True,
            timeout=20,
        )
        if proc.returncode == 0:
            return
        time.sleep(0.5)
    raise RuntimeError(f"timed out waiting for {url}")


def wait_exec(name: str, command: list[str], timeout_s: int = 60) -> None:
    deadline = time.time() + timeout_s
    last_output = ""
    while time.time() < deadline:
        proc = run(["container", "exec", name, *command], check=False, capture=True, timeout=20)
        last_output = (proc.stdout or "").strip()
        if proc.returncode == 0:
            return
        time.sleep(0.75)
    suffix = f": {last_output}" if last_output else ""
    raise RuntimeError(f"timed out waiting for {name}: {' '.join(command)}{suffix}")


def logs_tail(name: str, lines: int = 120) -> str:
    proc = run(["container", "logs", "-n", str(lines), name], check=False, capture=True)
    return (proc.stdout or "").strip()


def build_turbodb(target: str, prefix: str | None = None) -> None:
    argv = ["zig", "build"]
    if prefix:
        argv.extend(["--prefix", prefix])
    argv.append(f"-Dtarget={target}")
    run(argv)


@dataclass
class Service:
    name: str
    host: str
    error: str | None = None


class BenchRun:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.run_id = args.run_id or f"shape-{uuid.uuid4().hex[:10]}"
        self.network = args.network or f"tdb-bench-{self.run_id}"
        self.output = Path(args.output or ROOT / "benchmark-results" / f"{self.run_id}.json").resolve()
        self.containers: list[str] = []
        self.volumes: list[str] = []

    def name(self, suffix: str) -> str:
        return f"tdb-{self.run_id}-{suffix}"

    def volume(self, suffix: str) -> str:
        name = self.name(suffix)
        delete_volume(name)
        create_volume(name)
        self.volumes.append(name)
        return name

    def start(self) -> dict[str, Service]:
        create_network(self.network)
        services: dict[str, Service] = {}

        if not self.args.skip_turbodb:
            build_turbodb(self.args.turbodb_target)
        if not self.args.skip_turbodb_ffi:
            build_turbodb(self.args.turbodb_ffi_target, self.args.turbodb_ffi_prefix)
        if not self.args.skip_turbodb:
            services["turbodb"] = self.start_turbodb()
        if not self.args.skip_postgres:
            services["postgres"] = self.start_postgres()
        if not self.args.skip_mysql:
            services["mysql"] = self.start_mysql()
        if not self.args.skip_tigerbeetle:
            services["tigerbeetle"] = self.start_tigerbeetle()
        return services

    def start_turbodb(self) -> Service:
        name = self.name("turbodb")
        data_volume = self.volume("turbodb-data")
        delete_container(name)
        cmd = [
            "container", "run", "-d",
            "--name", name,
            "--network", self.network,
            "--mount", f"type=bind,source={ROOT},target=/work",
            "--volume", f"{data_volume}:/data",
            "--entrypoint", "/work/zig-out/bin/turbodb",
            "alpine:3.20",
            "--data", "/data",
            "--port", "27017",
            "--http",
        ]
        if self.args.turbodb_http_runtime:
            cmd.extend(["--http-runtime", self.args.turbodb_http_runtime])
        run(cmd)
        self.containers.append(name)
        host = container_ip(name)
        wait_http(self.network, f"http://{host}:27017/health", timeout_s=60)
        return Service(name=name, host=host)

    def start_postgres(self) -> Service:
        name = self.name("postgres")
        data_volume = self.volume("postgres-data")
        delete_container(name)
        run([
            "container", "run", "-d",
            "--name", name,
            "--network", self.network,
            "-e", "POSTGRES_PASSWORD=postgres",
            "-e", "POSTGRES_DB=bench",
            "--volume", f"{data_volume}:/var/lib/postgresql",
            self.args.postgres_image,
        ])
        self.containers.append(name)
        try:
            wait_exec(name, ["pg_isready", "-U", "postgres", "-d", "bench"], timeout_s=90)
        except Exception as exc:
            raise RuntimeError(f"{exc}\npostgres logs:\n{logs_tail(name)}") from exc
        return Service(name=name, host=container_ip(name))

    def start_mysql(self) -> Service:
        name = self.name("mysql")
        data_volume = self.volume("mysql-data")
        delete_container(name)
        run([
            "container", "run", "-d",
            "--name", name,
            "--network", self.network,
            "-e", "MYSQL_ROOT_PASSWORD=mysql",
            "-e", "MYSQL_DATABASE=bench",
            "--volume", f"{data_volume}:/var/lib/mysql",
            self.args.mysql_image,
        ])
        self.containers.append(name)
        try:
            wait_exec(name, ["mysqladmin", "ping", "-uroot", "-pmysql", "--silent"], timeout_s=120)
        except Exception as exc:
            raise RuntimeError(f"{exc}\nmysql logs:\n{logs_tail(name)}") from exc
        return Service(name=name, host=container_ip(name))

    def start_tigerbeetle(self) -> Service:
        name = self.name("tigerbeetle")
        data_volume = self.volume("tigerbeetle-data")
        data_file = "/data/0_0.tigerbeetle"
        delete_container(name)

        format_proc = run([
            "container", "run", "--rm",
            "--volume", f"{data_volume}:/data",
            self.args.tigerbeetle_image,
            "format",
            "--cluster=0",
            "--replica=0",
            "--replica-count=1",
            "--development",
            data_file,
        ], check=False, capture=True, timeout=120)
        if format_proc.returncode != 0:
            message = format_proc.stdout.strip() if format_proc.stdout else "format failed"
            if self.args.require_tigerbeetle:
                raise RuntimeError(message)
            return Service(name=name, host="", error=message)

        start_proc = run([
            "container", "run", "-d",
            "--name", name,
            "--network", self.network,
            "--memory", self.args.tigerbeetle_memory,
            "--ulimit", "memlock=-1:-1",
            "--volume", f"{data_volume}:/data",
            self.args.tigerbeetle_image,
            "start",
            "--addresses=0.0.0.0:3000",
            "--development",
            data_file,
        ], check=False, capture=True, timeout=120)
        if start_proc.returncode != 0:
            message = start_proc.stdout.strip() if start_proc.stdout else "start failed"
            if self.args.require_tigerbeetle:
                raise RuntimeError(message)
            return Service(name=name, host="", error=message)

        self.containers.append(name)
        time.sleep(2.0)
        try:
            host = container_ip(name)
        except Exception as exc:
            message = logs_tail(name) or str(exc)
            if self.args.require_tigerbeetle:
                raise RuntimeError(f"TigerBeetle did not stay running:\n{message}") from exc
            return Service(name=name, host="", error=message)
        return Service(name=name, host=host)

    def run_client(self, services: dict[str, Service]) -> int:
        self.output.parent.mkdir(parents=True, exist_ok=True)
        deps: list[str] = []
        args = [
            "python", "bench/container_shape_bench.py",
            "--users", str(self.args.users),
            "--orders-per-user", str(self.args.orders_per_user),
            "--samples", str(self.args.samples),
            "--batch-size", str(self.args.batch_size),
            "--output", f"/work/{self.output.relative_to(ROOT)}",
            "--turbodb-bulk-mode", self.args.turbodb_bulk_mode,
        ]

        if (svc := services.get("turbodb")) and not svc.error:
            args.extend(["--turbodb-host", svc.host])
        if not self.args.skip_turbodb_ffi:
            args.extend([
                "--turbodb-ffi-lib", self.args.turbodb_ffi_lib,
                "--turbodb-ffi-dir", self.args.turbodb_ffi_dir,
            ])
        if (svc := services.get("postgres")) and not svc.error:
            deps.append("psycopg[binary]")
            args.extend(["--postgres-host", svc.host])
        if (svc := services.get("mysql")) and not svc.error:
            deps.append("pymysql")
            deps.append("cryptography")
            args.extend(["--mysql-host", svc.host])
        if (svc := services.get("tigerbeetle")) and not svc.error:
            deps.append("tigerbeetle")
            args.extend(["--tigerbeetle-host", svc.host])

        has_service_engine = any(services.get(engine) and not services[engine].error for engine in services)
        if not has_service_engine and self.args.skip_turbodb_ffi:
            raise RuntimeError("no benchmark services started successfully")

        client_name = self.name("client")
        delete_container(client_name)
        pip_cmd = "python -m pip install --quiet --disable-pip-version-check --root-user-action=ignore " + " ".join(deps) if deps else "true"
        bench_cmd = shlex_join(args)
        command = f"{pip_cmd} && {bench_cmd}"
        proc = run([
            "container", "run", "--rm",
            "--name", client_name,
            "--network", self.network,
            "--mount", f"type=bind,source={ROOT},target=/work",
            "-w", "/work",
            "--entrypoint", "/bin/sh",
            self.args.python_image,
            "-lc", command,
        ], check=False)
        return proc.returncode

    def cleanup(self) -> None:
        if self.args.keep:
            print(f"keeping containers/network/volumes for run {self.run_id}", flush=True)
            return
        for name in reversed(self.containers):
            stop_container(name)
            delete_container(name)
        delete_network(self.network)
        for name in reversed(self.volumes):
            delete_volume(name)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run TurboDB/PostgreSQL/MySQL/TigerBeetle benchmarks in Apple containers")
    ap.add_argument("--users", type=int, default=1000)
    ap.add_argument("--orders-per-user", type=int, default=3)
    ap.add_argument("--samples", type=int, default=500)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--output")
    ap.add_argument("--run-id")
    ap.add_argument("--network")
    ap.add_argument("--turbodb-target", default="aarch64-linux-musl")
    ap.add_argument("--turbodb-ffi-target", default="aarch64-linux-gnu")
    ap.add_argument("--turbodb-ffi-prefix", default="zig-out-ffi")
    ap.add_argument("--turbodb-ffi-lib", default="/work/zig-out-ffi/lib/libturbodb.so")
    ap.add_argument("--turbodb-ffi-dir", default="/tmp/turbodb_ffi_shape_bench")
    ap.add_argument("--turbodb-bulk-mode", choices=("ndjson", "binary"), default="ndjson")
    ap.add_argument("--turbodb-http-runtime", choices=("threaded", "nanoapi-raw"), default=None)
    ap.add_argument("--python-image", default="python:3.12-slim")
    ap.add_argument("--postgres-image", default="postgres:18")
    ap.add_argument("--mysql-image", default="mysql:8.4")
    ap.add_argument("--tigerbeetle-image", default="ghcr.io/tigerbeetle/tigerbeetle:latest")
    ap.add_argument("--tigerbeetle-memory", default="2G")
    ap.add_argument("--skip-turbodb", action="store_true")
    ap.add_argument("--skip-turbodb-ffi", action="store_true")
    ap.add_argument("--skip-postgres", action="store_true")
    ap.add_argument("--skip-mysql", action="store_true")
    ap.add_argument("--skip-tigerbeetle", action="store_true")
    ap.add_argument("--require-tigerbeetle", action="store_true")
    ap.add_argument("--keep", action="store_true", help="leave containers, network, and volumes in place")
    return ap.parse_args()


def main() -> int:
    if not shutil.which("container"):
        print("Apple container CLI is required: https://github.com/apple/container", file=sys.stderr)
        return 2

    args = parse_args()
    bench = BenchRun(args)
    services: dict[str, Service] = {}
    try:
        services = bench.start()
        errors = {name: svc.error for name, svc in services.items() if svc.error}
        if errors:
            print("optional services failed:")
            for name, error in errors.items():
                print(f"  {name}: {error}")
        rc = bench.run_client(services)
        print(f"benchmark output: {bench.output}")
        return rc
    finally:
        bench.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
