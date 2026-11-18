"""Raw NSQ V2 TCP consumer and producer."""

from __future__ import annotations

import json
import logging
import socket
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from task_tools.bus.base import BusOptions

FRAME_TYPE_RESPONSE = 0
FRAME_TYPE_ERROR = 1
FRAME_TYPE_MESSAGE = 2
MAGIC = b"  V2"
logger = logging.getLogger(__name__)


def _read_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("nsq connection closed")
        buf.extend(chunk)
    return bytes(buf)


def _parse_host(host: str) -> tuple[str, int]:
    if host.startswith("["):
        h, _, p = host.partition("]:")
        return h[1:], int(p.lstrip(":") or 4150)
    if ":" in host:
        h, p = host.rsplit(":", 1)
        return h, int(p)
    return host, 4150


def lookup_nsqd_hosts(lookupd_hosts: list[str], topic: str) -> list[str]:
    hosts: list[str] = []
    for ld in lookupd_hosts:
        if "://" not in ld:
            ld = f"http://{ld}"
        url = f"{ld.rstrip('/')}/lookup?topic={urllib.parse.quote(topic)}"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.loads(resp.read().decode())
            for pr in data.get("producers", []):
                addr = f"{pr['broadcast_address']}:{pr['tcp_port']}"
                hosts.append(addr)
        except (urllib.error.URLError, KeyError, json.JSONDecodeError) as exc:
            logger.warning("lookupd %s: %s", ld, exc)
    return hosts


class _NSQConnection:
    def __init__(self, host: str) -> None:
        self.host = host
        self._sock: socket.socket | None = None
        self._lock = threading.Lock()

    def connect(self) -> None:
        h, port = _parse_host(self.host if ":" in self.host else f"{self.host}:4150")
        self._sock = socket.create_connection((h, port), timeout=30)
        self._sock.sendall(MAGIC)
        body = json.dumps(
            {
                "client_id": "task_tools",
                "hostname": "task_tools",
                "heartbeat_interval": 30000,
                "feature_negotiation": True,
            }
        ).encode()
        self._sock.sendall(b"IDENTIFY\n")
        self._sock.sendall(struct.pack(">I", len(body)))
        self._sock.sendall(body)
        self._read_response_frame()

    def _read_response_frame(self) -> bytes:
        assert self._sock
        size = struct.unpack(">I", _read_exact(self._sock, 4))[0]
        frame_type = struct.unpack(">I", _read_exact(self._sock, 4))[0]
        payload = _read_exact(self._sock, size - 4)
        if frame_type == FRAME_TYPE_ERROR:
            raise RuntimeError(payload.decode("utf-8", "replace"))
        return payload

    def pub(self, topic: str, msg: bytes) -> None:
        assert self._sock
        with self._lock:
            self._sock.sendall(f"PUB {topic}\n".encode())
            self._sock.sendall(struct.pack(">I", len(msg)))
            self._sock.sendall(msg)

    def sub(self, topic: str, channel: str) -> None:
        assert self._sock
        with self._lock:
            self._sock.sendall(f"SUB {topic} {channel}\n".encode())

    def rdy(self, count: int) -> None:
        assert self._sock
        with self._lock:
            self._sock.sendall(f"RDY {count}\n".encode())

    def fin(self, msg_id: bytes) -> None:
        assert self._sock
        with self._lock:
            self._sock.sendall(b"FIN " + msg_id + b"\n")

    def req(self, msg_id: bytes, delay_ms: int = -1) -> None:
        assert self._sock
        with self._lock:
            self._sock.sendall(b"REQ " + msg_id + f" {delay_ms}\n".encode())

    def read_message(self) -> tuple[bytes, bytes] | None:
        assert self._sock
        with self._lock:
            while True:
                size = struct.unpack(">I", _read_exact(self._sock, 4))[0]
                frame_type = struct.unpack(">I", _read_exact(self._sock, 4))[0]
                payload = _read_exact(self._sock, size - 4)
                if frame_type == FRAME_TYPE_RESPONSE:
                    text = payload.decode("utf-8", "replace").strip()
                    if text == "_heartbeat_":
                        self._sock.sendall(b"NOP\n")
                        continue
                    continue
                if frame_type == FRAME_TYPE_ERROR:
                    raise RuntimeError(payload.decode("utf-8", "replace"))
                if frame_type == FRAME_TYPE_MESSAGE:
                    msg_id = payload[0:16]
                    body = payload[26:]
                    return msg_id, body
                if frame_type == 1:
                    self._sock.sendall(b"NOP\n")
                    continue

    def close(self) -> None:
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None


class NSQProducer:
    def __init__(self, opt: BusOptions) -> None:
        hosts = list(opt.nsqd_hosts or ["localhost:4150"])
        self._hosts = hosts
        self._index = 0
        self._conns: dict[str, _NSQConnection] = {}
        self._info: dict[str, Any] = {"bus": "nsq", "sent": {}}
        for h in hosts:
            conn = _NSQConnection(h)
            conn.connect()
            self._conns[h] = conn

    def send(self, topic: str, msg: bytes) -> Exception | None:
        if not self._hosts:
            return RuntimeError("no nsqd hosts")
        host = self._hosts[self._index % len(self._hosts)]
        self._index += 1
        conn = self._conns.get(host)
        if not conn:
            return RuntimeError(f"no connection to {host}")
        try:
            conn.pub(topic, msg)
            sent = self._info.setdefault("sent", {})
            sent[topic] = int(sent.get(topic, 0)) + 1
            return None
        except Exception as exc:
            return exc

    def stop(self) -> Exception | None:
        for conn in self._conns.values():
            conn.close()
        return None

    def info(self) -> dict[str, Any]:
        return dict(self._info)


class NSQConsumer:
    def __init__(self, topic: str, channel: str, opt: BusOptions) -> None:
        self._topic = topic
        self._channel = channel or topic
        self._opt = opt
        self._stopped = threading.Event()
        self._msg_requested = 0
        self._msg_received = 0
        self._lock = threading.Lock()
        self._pending: list[tuple[bytes, bytes, _NSQConnection]] = []
        self._conn: _NSQConnection | None = None
        self._reader: threading.Thread | None = None
        self._info: dict[str, Any] = {"bus": "nsq", "topic": topic, "channel": self._channel, "received": 0}
        self._connect()

    def _pick_host(self) -> str:
        if self._opt.lookupd_hosts:
            hosts = lookup_nsqd_hosts(self._opt.lookupd_hosts, self._topic)
            if hosts:
                return hosts[0]
        if self._opt.nsqd_hosts:
            return self._opt.nsqd_hosts[0]
        return "localhost:4150"

    def _connect(self) -> None:
        host = self._pick_host()
        self._conn = _NSQConnection(host)
        self._conn.connect()
        self._conn.sub(self._topic, self._channel)
        self._conn.rdy(0)
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _read_loop(self) -> None:
        assert self._conn
        while not self._stopped.is_set():
            try:
                item = self._conn.read_message()
                if item is None:
                    continue
                msg_id, body = item
                with self._lock:
                    if self._msg_received >= self._msg_requested:
                        self._conn.req(msg_id)
                        continue
                    self._pending.append((msg_id, body, self._conn))
                    self._msg_received += 1
                    if self._msg_received >= self._msg_requested:
                        self._conn.rdy(0)
            except Exception as exc:
                if not self._stopped.is_set():
                    logger.error("nsq read: %s", exc)
                    time.sleep(1)

    def msg(self) -> tuple[bytes, bool, Exception | None]:
        if self._stopped.is_set():
            return b"", True, None
        with self._lock:
            self._msg_requested += 1
            if self._conn:
                self._conn.rdy(1)
        deadline = time.time() + 300
        while time.time() < deadline and not self._stopped.is_set():
            with self._lock:
                if self._pending:
                    msg_id, body, conn = self._pending.pop(0)
                    conn.fin(msg_id)
                    self._info["received"] = int(self._info["received"]) + 1
                    return body, False, None
            time.sleep(0.01)
        if self._stopped.is_set():
            return b"", True, None
        return b"", False, None

    def stop(self) -> Exception | None:
        self._stopped.set()
        if self._conn:
            self._conn.rdy(0)
            self._conn.close()
        return None

    def info(self) -> dict[str, Any]:
        return dict(self._info)