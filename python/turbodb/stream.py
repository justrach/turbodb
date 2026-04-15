"""WebSocket streaming bulk insert for TurboDB.

Fire-and-forget protocol: client streams NDJSON frames without waiting
for per-frame responses. ~10-20x faster than HTTP batches over high-latency
connections (SSH tunnels, WAN).

Usage::

    from turbodb.stream import StreamInserter

    with StreamInserter("ws://host:port", "mycollection") as s:
        for doc in docs:
            s.add(doc["key"], doc["value"])
    print(s.result)  # {"inserted": 12000, "errors": 0, "frames": 6}

Requires: pip install websocket-client
"""

import json

try:
    import websocket
except ImportError:
    websocket = None


class StreamInserter:
    """Fire-and-forget WebSocket streaming bulk inserter."""

    def __init__(self, url, collection, tenant=None, batch_size=2000):
        if websocket is None:
            raise ImportError("pip install websocket-client")
        self.url = url
        self.collection = collection
        self.tenant = tenant
        self.batch_size = batch_size
        self._ws = None
        self._buf = []
        self._sent_frames = 0
        self.result = None

    def open(self):
        """Connect and enter STREAM mode."""
        self._ws = websocket.create_connection(self.url)
        init = f"STREAM /db/{self.collection}/bulk"
        if self.tenant:
            init += f" tenant={self.tenant}"
        self._ws.send(init)
        ack = json.loads(self._ws.recv())
        if "error" in ack:
            raise RuntimeError(f"Stream init failed: {ack['error']}")
        return self

    def add(self, key, value):
        """Buffer a document. Auto-flushes at batch_size."""
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        self._buf.append(json.dumps({"key": key, "value": value}))
        if len(self._buf) >= self.batch_size:
            self.flush()

    def send_ndjson(self, ndjson_str):
        """Send a pre-built NDJSON string as one frame (fire-and-forget)."""
        self._ws.send(ndjson_str)
        self._sent_frames += 1

    def flush(self):
        """Send buffered docs as one NDJSON frame."""
        if not self._buf:
            return
        self._ws.send("\n".join(self._buf))
        self._sent_frames += 1
        self._buf.clear()

    def close(self):
        """Flush remaining, close WS, return final summary."""
        self.flush()
        self._ws.send_close()
        try:
            self.result = json.loads(self._ws.recv())
        except Exception:
            self.result = {"inserted": 0, "errors": 0, "frames": self._sent_frames}
        self._ws.close()
        return self.result

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args):
        self.close()
