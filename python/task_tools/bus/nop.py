"""nop bus for testing."""

from __future__ import annotations

from typing import Any


class NullProducer:
    def send(self, topic: str, msg: bytes) -> Exception | None:
        return None

    def stop(self) -> Exception | None:
        return None

    def info(self) -> dict[str, Any]:
        return {"bus": "null", "sent": {}}


def new_nop_producer(mock: str) -> NullProducer:
    if mock == "init_err":
        raise RuntimeError("nop init_err")
    if mock == "send_err":
        return _ErrSendProducer()
    return NullProducer()


class _ErrSendProducer(NullProducer):
    def send(self, topic: str, msg: bytes) -> Exception | None:
        return RuntimeError("send_err")


def new_nop_consumer(mock: str):
    if mock == "init_err":
        raise RuntimeError("nop init_err")
    if mock == "msg_done":
        return _StaticConsumer(b"", True, None)
    if mock == "msg_msg_done":
        return _StaticConsumer(b'{"type":"t","info":"i"}', True, None)
    if mock == "msg_err":
        return _StaticConsumer(b"", False, RuntimeError("msg_err"))
    return _StaticConsumer(b"", True, None)


class _StaticConsumer:
    def __init__(self, msg: bytes, done: bool, err: Exception | None) -> None:
        self._msg = msg
        self._done = done
        self._err = err
        self._called = False

    def msg(self) -> tuple[bytes, bool, Exception | None]:
        if self._called:
            return b"", True, None
        self._called = True
        return self._msg, self._done, self._err

    def stop(self) -> Exception | None:
        return None

    def info(self) -> dict[str, Any]:
        return {"bus": "nop", "received": 1 if self._called else 0}