#!/usr/bin/env python3
"""Run TurboDB behind a nanoapi proxy and pound only the proxy from a client container."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def shlex_join(argv: list[str]) -> str:
    import shlex

    return " ".join(shlex.quote(part) for part in argv)


def run(argv: list[str], *, check: bool = True, capture: bool = False, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
    print(f"+ {shlex_join(argv)}", flush=True)
    return subprocess.run(
        argv,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        check=check,
        timeout=timeout,
    )


def container_exists(name: str) -> bool:
    return run(["container", "inspect", name], check=False, capture=True).returncode == 0


def delete_container(name: str) -> None:
    if container_exists(name):
        run(["container", "stop", name], check=False, capture=True)
        run(["container", "rm", name], check=False, capture=True)


def network_exists(name: str) -> bool:
    proc = run(["container", "network", "list"], check=False, capture=True)
    return proc.returncode == 0 and proc.stdout is not None and any(line.split(maxsplit=1)[0] == name for line in proc.stdout.splitlines()[1:])


def create_network(name: str) -> None:
    if not network_exists(name):
        run(["container", "network", "create", name])


def delete_network(name: str) -> None:
    if network_exists(name):
        run(["container", "network", "delete", name], check=False, capture=True)


def volume_exists(name: str) -> bool:
    proc = run(["container", "volume", "list"], check=False, capture=True)
    return proc.returncode == 0 and proc.stdout is not None and any(line.split(maxsplit=1)[0] == name for line in proc.stdout.splitlines()[1:])


def create_volume(name: str) -> None:
    if not volume_exists(name):
        run(["container", "volume", "create", name])


def delete_volume(name: str) -> None:
    if volume_exists(name):
        run(["container", "volume", "delete", name], check=False, capture=True)


def inspect_json(name: str) -> object:
    proc = run(["container", "inspect", name], capture=True)
    assert proc.stdout is not None
    return json.loads(proc.stdout)


def container_ip(name: str) -> str:
    data = inspect_json(name)
    candidates = data if isinstance(data, list) else [data]
    for item in candidates:
        if not isinstance(item, dict):
            continue
        for net in item.get("networks") or []:
            address = net.get("ipv4Address") or net.get("address")
            if address:
                return str(address).split("/", 1)[0]
    raise RuntimeError(f"could not find IPv4 address for {name}")


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


def logs_tail(name: str, lines: int = 160) -> str:
    proc = run(["container", "logs", "-n", str(lines), name], check=False, capture=True)
    return (proc.stdout or "").strip()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Agentic nanoapi-over-TurboDB container stress harness")
    ap.add_argument("--run-id", default=f"agentic-nanoapi-{int(time.time())}")
    ap.add_argument("--network")
    ap.add_argument("--output")
    ap.add_argument("--target", default="aarch64-linux-musl")
    ap.add_argument("--optimize", default="ReleaseFast")
    ap.add_argument("--agents", type=int, default=32)
    ap.add_argument("--rounds", type=int, default=20)
    ap.add_argument("--events-per-round", type=int, default=3)
    ap.add_argument("--context-limit", type=int, default=20)
    ap.add_argument("--proxy-runtime", choices=("auto", "io_uring", "thread_per_connection"), default="io_uring")
    ap.add_argument("--proxy-workers", type=int, default=1)
    ap.add_argument("--turbodb-runtime", choices=("threaded", "nanoapi-raw"), default="threaded")
    ap.add_argument("--python-image", default="python:3.12-slim")
    ap.add_argument("--keep", action="store_true")
    return ap.parse_args()


def main() -> int:
    if not shutil.which("container"):
        print("Apple container CLI is required", file=sys.stderr)
        return 2

    args = parse_args()
    run_id = args.run_id
    network = args.network or f"tdb-agentic-{run_id}"
    output = Path(args.output or ROOT / "benchmark-results" / f"{run_id}.json").resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    turbodb_name = f"tdb-{run_id}-db"
    proxy_name = f"tdb-{run_id}-proxy"
    client_name = f"tdb-{run_id}-client"
    volume = f"tdb-{run_id}-data"

    try:
        create_network(network)
        create_volume(volume)
        run(["zig", "build", f"-Dtarget={args.target}", f"-Doptimize={args.optimize}"])

        delete_container(turbodb_name)
        run([
            "container", "run", "-d",
            "--name", turbodb_name,
            "--network", network,
            "--mount", f"type=bind,source={ROOT},target=/work,readonly",
            "--volume", f"{volume}:/data",
            "--entrypoint", "/work/zig-out/bin/turbodb",
            "alpine:3.20",
            "--data", "/data",
            "--port", "27017",
            "--http",
            "--http-runtime", args.turbodb_runtime,
        ])
        turbodb_host = container_ip(turbodb_name)
        wait_http(network, f"http://{turbodb_host}:27017/health", timeout_s=60)

        delete_container(proxy_name)
        run([
            "container", "run", "-d",
            "--name", proxy_name,
            "--network", network,
            "--mount", f"type=bind,source={ROOT},target=/work,readonly",
            "--entrypoint", "/work/zig-out/bin/nanoapi-agent-proxy",
            "alpine:3.20",
            "--port", "28080",
            "--turbodb-host", turbodb_host,
            "--turbodb-port", "27017",
            "--runtime", args.proxy_runtime,
            "--workers", str(args.proxy_workers),
        ])
        proxy_host = container_ip(proxy_name)
        wait_http(network, f"http://{proxy_host}:28080/health", timeout_s=60)
        wait_http(network, f"http://{proxy_host}:28080/turbodb/health", timeout_s=60)

        delete_container(client_name)
        proc = run([
            "container", "run", "--rm",
            "--name", client_name,
            "--network", network,
            "--mount", f"type=bind,source={ROOT},target=/work",
            "-w", "/work",
            "--entrypoint", "python",
            args.python_image,
            "bench/agentic_nanoapi_client.py",
            "--host", proxy_host,
            "--port", "28080",
            "--agents", str(args.agents),
            "--rounds", str(args.rounds),
            "--events-per-round", str(args.events_per_round),
            "--context-limit", str(args.context_limit),
            "--run-id", run_id,
            "--output", f"/work/{output.relative_to(ROOT)}",
        ], check=False)

        if proc.returncode != 0:
            print("\nproxy logs:\n" + logs_tail(proxy_name), file=sys.stderr)
            print("\nturbodb logs:\n" + logs_tail(turbodb_name), file=sys.stderr)
        print(f"benchmark output: {output}")
        return proc.returncode
    finally:
        if args.keep:
            print(f"keeping network={network} db={turbodb_name} proxy={proxy_name} volume={volume}")
        else:
            delete_container(client_name)
            delete_container(proxy_name)
            delete_container(turbodb_name)
            delete_network(network)
            delete_volume(volume)


if __name__ == "__main__":
    raise SystemExit(main())
