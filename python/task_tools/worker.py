"""Worker interface and helpers."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from typing import Callable

from task_tools.task import Result


class Context:
    """Cancellation token, similar to context.Context."""

    def __init__(self, event: threading.Event | None = None) -> None:
        self._event = event or threading.Event()

    def done(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)


def is_done(ctx: Context) -> bool:
    return ctx.done()


def interrupted() -> tuple[Result, str]:
    return Result.ERROR, "task interrupted"


def completed(fmt: str, *args: object) -> tuple[Result, str]:
    return Result.COMPLETE, fmt % args if args else fmt


def failed(err: BaseException) -> tuple[Result, str]:
    return Result.ERROR, str(err)


def failf(fmt: str, *args: object) -> tuple[Result, str]:
    return Result.ERROR, fmt % args


def warn(fmt: str, *args: object) -> tuple[Result, str]:
    return Result.WARN, fmt % args if args else fmt


def alert(fmt: str, *args: object) -> tuple[Result, str]:
    return Result.ALERT, fmt % args if args else fmt


class Worker(ABC):
    @abstractmethod
    def do_task(self, ctx: Context) -> tuple[Result, str]:
        """Run the task; return result and message."""

    def get_meta(self) -> dict[str, list[str]] | None:
        return None


class InvalidWorker(Worker):
    def __init__(self, message: str) -> None:
        self._message = message

    def do_task(self, ctx: Context) -> tuple[Result, str]:
        return Result.ERROR, self._message


def invalid_worker(fmt: str, *args: object) -> Worker:
    return InvalidWorker(fmt % args if args else fmt)


def is_invalid_worker(w: Worker) -> tuple[bool, str]:
    if isinstance(w, InvalidWorker):
        return True, w._message
    return False, ""


NewWorker = Callable[[str], Worker]