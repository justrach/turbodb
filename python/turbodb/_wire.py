"""TurboDB binary wire protocol client."""

import socket
import struct
import threading

# Op codes (must match wire.zig)
OP_INSERT = 0x01
OP_GET    = 0x02
OP_UPDATE = 0x03
OP_DELETE = 0x04
OP_SCAN   = 0x05
OP_PING   = 0x06
OP_BATCH  = 0x07

STATUS_OK        = 0x00
STATUS_NOT_FOUND = 0x01
STATUS_ERROR     = 0x02

HEADER_SIZE = 5  # 4 bytes len + 1 byte op


class WireConnection:
    """Single TCP connection to TurboDB wire protocol."""

    def __init__(self, host="127.0.0.1", port=27017):
        self.host = host
        self.port = port
        self._sock = None
        self._lock = threading.Lock()

    def connect(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock.connect((self.host, self.port))

    def close(self):
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def _ensure_connected(self):
        if not self._sock:
            self.connect()

    def _send_frame(self, op, payload=b""):
        frame_len = HEADER_SIZE + len(payload)
        header = struct.pack(">I", frame_len) + bytes([op])
        self._sock.sendall(header + payload)

    def _recv_frame(self):
        # Read 4-byte length
        data = self._recvall(4)
        if not data:
            raise ConnectionError("Connection closed")
        frame_len = struct.unpack(">I", data)[0]
        if frame_len < HEADER_SIZE:
            raise ValueError(f"Invalid frame length: {frame_len}")
        # Read rest of frame
        rest = self._recvall(frame_len - 4)
        op = rest[0]
        payload = rest[1:]
        return op, payload

    def _recvall(self, n):
        data = bytearray()
        while len(data) < n:
            chunk = self._sock.recv(n - len(data))
            if not chunk:
                return None
            data.extend(chunk)
        return bytes(data)

    # ── Operations ───────────────────────────────────────────────────────

    def ping(self):
        with self._lock:
            self._ensure_connected()
            self._send_frame(OP_PING)
            op, payload = self._recv_frame()
            return payload[0] == STATUS_OK

    def insert(self, collection, key, value):
        col = collection.encode() if isinstance(collection, str) else collection
        k = key.encode() if isinstance(key, str) else key
        v = value.encode() if isinstance(value, str) else value

        payload = struct.pack("<H", len(col)) + col
        payload += struct.pack("<H", len(k)) + k
        payload += struct.pack("<I", len(v)) + v

        with self._lock:
            self._ensure_connected()
            self._send_frame(OP_INSERT, payload)
            op, resp = self._recv_frame()
            if resp[0] != STATUS_OK:
                return None
            doc_id = struct.unpack("<Q", resp[1:9])[0]
            return doc_id

    def get(self, collection, key):
        col = collection.encode() if isinstance(collection, str) else collection
        k = key.encode() if isinstance(key, str) else key

        payload = struct.pack("<H", len(col)) + col
        payload += struct.pack("<H", len(k)) + k

        with self._lock:
            self._ensure_connected()
            self._send_frame(OP_GET, payload)
            op, resp = self._recv_frame()
            if resp[0] == STATUS_NOT_FOUND:
                return None
            if resp[0] != STATUS_OK:
                return None
            doc_id = struct.unpack("<Q", resp[1:9])[0]
            version = resp[9]
            val_len = struct.unpack("<I", resp[10:14])[0]
            value = resp[14:14 + val_len]
            return {
                "doc_id": doc_id,
                "version": version,
                "value": value.decode("utf-8", errors="replace"),
            }

    def update(self, collection, key, value):
        col = collection.encode() if isinstance(collection, str) else collection
        k = key.encode() if isinstance(key, str) else key
        v = value.encode() if isinstance(value, str) else value

        payload = struct.pack("<H", len(col)) + col
        payload += struct.pack("<H", len(k)) + k
        payload += struct.pack("<I", len(v)) + v

        with self._lock:
            self._ensure_connected()
            self._send_frame(OP_UPDATE, payload)
            op, resp = self._recv_frame()
            return resp[0] == STATUS_OK

    def delete(self, collection, key):
        col = collection.encode() if isinstance(collection, str) else collection
        k = key.encode() if isinstance(key, str) else key

        payload = struct.pack("<H", len(col)) + col
        payload += struct.pack("<H", len(k)) + k

        with self._lock:
            self._ensure_connected()
            self._send_frame(OP_DELETE, payload)
            op, resp = self._recv_frame()
            return resp[0] == STATUS_OK


class WirePool:
    """Connection pool for TurboDB wire protocol."""

    def __init__(self, host="127.0.0.1", port=27017, size=4):
        self.host = host
        self.port = port
        self._conns = []
        self._idx = 0
        self._lock = threading.Lock()
        for _ in range(size):
            c = WireConnection(host, port)
            c.connect()
            self._conns.append(c)

    def get_conn(self):
        with self._lock:
            c = self._conns[self._idx % len(self._conns)]
            self._idx += 1
            return c

    def close(self):
        for c in self._conns:
            c.close()
