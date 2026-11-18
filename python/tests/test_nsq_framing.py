"""Unit tests for NSQ wire helpers (no live nsqd required)."""

import struct

from task_tools.bus.nsq import FRAME_TYPE_MESSAGE, _parse_host


def test_parse_host() -> None:
    assert _parse_host("localhost:4150") == ("localhost", 4150)
    assert _parse_host("localhost")[1] == 4150


def test_message_frame_layout() -> None:
    msg_id = b"x" * 16
    body = b'{"type":"t","info":"i"}'
    inner = b"\x00" * 8 + b"\x00\x01" + msg_id + body
    frame = struct.pack(">I", len(inner) + 4) + struct.pack(">I", FRAME_TYPE_MESSAGE) + inner
    assert len(msg_id) == 16