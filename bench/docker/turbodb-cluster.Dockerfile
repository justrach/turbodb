# TurboDB — Pre-built binary container for cluster testing
# Build the binaries on the host first (zig build), then copy them in.
#
# Usage:
#   zig build -Doptimize=ReleaseFast -Dtarget=aarch64-linux
#   docker build -f bench/docker/turbodb-cluster.Dockerfile -t turbodb-node .
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates netcat-openbsd && rm -rf /var/lib/apt/lists/*

# Copy pre-built binaries (built on host with cross-compilation)
COPY zig-out/bin/turbodb /usr/local/bin/turbodb
COPY zig-out/bin/test-calvin /usr/local/bin/test-calvin

RUN mkdir -p /data && chmod +x /usr/local/bin/turbodb /usr/local/bin/test-calvin
VOLUME /data

ENTRYPOINT ["turbodb"]
CMD ["--data", "/data", "--port", "27017"]
