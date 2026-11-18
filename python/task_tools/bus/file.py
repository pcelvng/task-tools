"""file bus implementations."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, TextIO


class FileConsumer:
    def __init__(self, path: str) -> None:
        if not path:
            raise ValueError("path required")
        self._path = path
        self._file: TextIO = open(path, encoding="utf-8")  # noqa: SIM115
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._info: dict[str, Any] = {"bus": path, "received": 0}

    def msg(self) -> tuple[bytes, bool, Exception | None]:
        if self._stopped.is_set():
            return b"", True, None
        with self._lock:
            line = self._file.readline()
            if not line:
                return b"", True, None
            self._info["received"] = int(self._info["received"]) + 1
            return line.rstrip("\n").encode(), False, None

    def stop(self) -> Exception | None:
        if self._stopped.is_set():
            return None
        self._stopped.set()
        if not self._path.startswith("/dev/"):
            self._file.close()
        return None

    def info(self) -> dict[str, Any]:
        return dict(self._info)


class FileProducer:
    """Append-only producer; topic is the file path."""

    def __init__(self) -> None:
        self._handles: dict[str, TextIO] = {}
        self._info: dict[str, Any] = {"bus": "file", "sent": {}}

    def send(self, topic: str, msg: bytes) -> Exception | None:
        if topic not in self._handles:
            self._handles[topic] = open(topic, "ab")  # noqa: SIM115
        self._handles[topic].write(msg)
        self._handles[topic].write(b"\n")
        self._handles[topic].flush()
        sent = self._info.setdefault("sent", {})
        sent[topic] = int(sent.get(topic, 0)) + 1
        return None

    def stop(self) -> Exception | None:
        for h in self._handles.values():
            h.close()
        return None

    def info(self) -> dict[str, Any]:
        return dict(self._info)