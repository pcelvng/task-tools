"""Task JSON codec compatible with github.com/pcelvng/task."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode


class Result(str, Enum):
    COMPLETE = "complete"
    ERROR = "error"
    ALERT = "alert"
    WARN = "warn"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _format_rfc3339(when: datetime) -> str:
    when = when.astimezone(timezone.utc).replace(microsecond=0)
    return when.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_rfc3339(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc).replace(microsecond=0)


@dataclass
class Task:
    type: str
    info: str
    job: str = ""
    created: str = ""
    id: str = ""
    meta: str = ""
    result: Result | str = ""
    msg: str = ""
    started: str = ""
    ended: str = ""

    _created: datetime | None = field(default=None, repr=False, compare=False)
    _started: datetime | None = field(default=None, repr=False, compare=False)
    _ended: datetime | None = field(default=None, repr=False, compare=False)

    @classmethod
    def new(cls, tsk_type: str, info: str) -> Task:
        now = _utc_now()
        return cls(
            type=tsk_type,
            info=info,
            id=str(uuid.uuid4()),
            created=_format_rfc3339(now),
            _created=now,
        )

    @classmethod
    def new_with_id(cls, tsk_type: str, info: str, tsk_id: str) -> Task:
        now = _utc_now()
        return cls(
            type=tsk_type,
            info=info,
            id=tsk_id,
            created=_format_rfc3339(now),
            _created=now,
        )

    def start(self) -> datetime:
        now = _utc_now()
        self._started = now
        self.started = _format_rfc3339(now)
        return now

    def end(self, result: Result | str, msg: str) -> datetime:
        if isinstance(result, Result):
            result = result.value
        self.result = result
        self.msg = msg
        now = _utc_now()
        self._ended = now
        self.ended = _format_rfc3339(now)
        return now

    def merge_meta(self, values: dict[str, list[str]]) -> None:
        """Merge worker meta into task meta (worker keys win). Mirrors Go launcher."""
        data: dict[str, list[str]] = {}
        if self.meta:
            for k, v in parse_qsl(self.meta, keep_blank_values=True):
                data.setdefault(k, []).append(v)
        for k, vs in values.items():
            data[k] = list(vs)
        encoded = urlencode(data, doseq=True)
        try:
            self.meta = unquote(encoded)
        except Exception:
            self.meta = encoded

    def json_bytes(self) -> bytes:
        obj: dict[str, Any] = {}
        obj["type"] = self.type
        if self.job:
            obj["job"] = self.job
        obj["info"] = self.info
        if self.created:
            obj["created"] = self.created
        if self.id:
            obj["id"] = self.id
        if self.meta:
            obj["meta"] = self.meta
        if self.result:
            obj["result"] = self.result.value if isinstance(self.result, Result) else self.result
        if self.msg:
            obj["msg"] = self.msg
        if self.started:
            obj["started"] = self.started
        if self.ended:
            obj["ended"] = self.ended
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode()

    def json_string(self) -> str:
        return self.json_bytes().decode()


def new_from_bytes(data: bytes | str) -> Task:
    raw = data if isinstance(data, str) else data.decode()
    parsed = json.loads(raw)
    tsk = Task(
        type=parsed.get("type", ""),
        info=parsed.get("info", ""),
        job=parsed.get("job", "") or "",
        created=parsed.get("created", "") or "",
        id=parsed.get("id", "") or "",
        meta=parsed.get("meta", "") or "",
        result=parsed.get("result", "") or "",
        msg=parsed.get("msg", "") or "",
        started=parsed.get("started", "") or "",
        ended=parsed.get("ended", "") or "",
    )

    for attr, key in (("_created", "created"), ("_started", "started"), ("_ended", "ended")):
        val = parsed.get(key, "")
        if val:
            try:
                dt = _parse_rfc3339(val)
                if dt.timestamp() == 0:
                    setattr(tsk, key, "")
                    setattr(tsk, attr, None)
                else:
                    setattr(tsk, attr, dt)
            except ValueError as exc:
                raise ValueError(f"invalid {key}: {exc}") from exc
    return tsk


def is_zero(tsk: Task) -> bool:
    return not tsk.type and not tsk.result and not tsk.created and not tsk.info