# Agent Runtime Rules

## Container-Only Runtime

Do not run TurboDB, `turbodb-gateway`, database services, long-lived servers,
stress tests, or deployment simulations directly on the host macOS machine.

Runtime work must use Apple `container`:

```sh
container --help
```

Reference: https://github.com/apple/container

Allowed on the host:

- Editing files.
- Inspecting files.
- Building binaries when no server is started.
- One-shot commands that do not bind ports, create launch agents, or start
  persistent services.

Not allowed on the host:

- `launchctl` service installation for TurboDB or its gateway.
- Starting `turbodb` or `turbodb-gateway` directly on macOS.
- Binding TurboDB, gateway, or test services to host ports.
- Moving live database data into host-managed service directories.

Before any database runtime, create or use an Apple `container` environment and
mount the intended data directory explicitly. Database updates must preserve
existing data: take a backup/snapshot first, reuse the existing data directory,
and never delete or reinitialize data unless the user explicitly asks for that.
