"""stdio bus implementations."""

from __future__ import annotations

import sys
import threading
from typing import Any


class StdinConsumer:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._info: dict[str, Any] = {"bus": "/dev/stdin", "received": 0}

    def msg(self) -> tuple[bytes, bool, Exception | None]:
        if self._stopped.is_set():
            return b"", True, None
        with self._lock:
            line = sys.stdin.buffer.readline()
            if not line:
                return b"", True, None
            self._info["received"] = int(self._info["received"]) + 1
            return line.rstrip(b"\n"), False, None

    def stop(self) -> Exception | None:
        self._stopped.set()
        return None

    def info(self) -> dict[str, Any]:
        return dict(self._info)


class StdoutProducer:
    def __init__(self) -> None:
        self._info: dict[str, Any] = {"bus": "stdout", "sent": {}}

    def send(self, topic: str, msg: bytes) -> Exception | None:
        sys.stdout.buffer.write(msg)
        sys.stdout.buffer.write(b"\n")
        sys.stdout.buffer.flush()
        sent = self._info.setdefault("sent", {})
        sent[topic] = int(sent.get(topic, 0)) + 1
        return None

    def stop(self) -> Exception | None:
        return None

    def info(self) -> dict[str, Any]:
        return dict(self._info)


class StderrProducer(StdoutProducer):
    def send(self, topic: str, msg: bytes) -> Exception | None:
        sys.stderr.buffer.write(msg)
        sys.stderr.buffer.write(b"\n")
        sys.stderr.buffer.flush()
        sent = self._info.setdefault("sent", {})
        sent[topic] = int(sent.get(topic, 0)) + 1
        return None