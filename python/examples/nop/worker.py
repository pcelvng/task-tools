"""nop worker — mirrors apps/workers/nop."""

from __future__ import annotations

import random
import threading
import time

from task_tools.task import Result
from task_tools.worker import Context, Worker, failed, interrupted


class NopOptions:
    fail_rate: int = 0
    dur: str = ""
    dur_variance: str = ""
    _dur: float = 0.0
    _dur_variance: float = 0.0

    def validate(self) -> None:
        if self.dur:
            self._dur = _parse_duration(self.dur)
        if self.dur_variance:
            self._dur_variance = _parse_duration(self.dur_variance)


def new_worker(_info: str, options: NopOptions) -> Worker:
    return _NopWorker(options)


class _NopWorker(Worker):
    def __init__(self, options: NopOptions) -> None:
        self._opt = options

    def do_task(self, ctx: Context) -> tuple[Result, str]:
        done = threading.Event()

        result: list[tuple[Result, str]] = []

        def run() -> None:
            result.append(self._do_task())
            done.set()

        threading.Thread(target=run, daemon=True).start()
        if not done.wait(timeout=max(self._opt._dur + self._opt._dur_variance + 1, 1)):
            if ctx.done():
                return interrupted()
        if ctx.done():
            return interrupted()
        return result[0] if result else (Result.ERROR, "no result")

    def _do_task(self) -> tuple[Result, str]:
        is_fail = _check_fail(self._opt.fail_rate)
        if is_fail:
            dur = _fail_duration(self._opt._dur, self._opt._dur_variance)
        else:
            dur = _success_duration(self._opt._dur, self._opt._dur_variance)
        if dur > 0:
            time.sleep(dur)
        if is_fail:
            return failed(RuntimeError("failed"))
        return Result.COMPLETE, "finished"


def _parse_duration(value: str) -> float:
    value = value.strip()
    if value.endswith("ms"):
        return float(value[:-2]) / 1000
    if value.endswith("s"):
        return float(value[:-1])
    if value.endswith("m"):
        return float(value[:-1]) * 60
    return float(value)


def _success_duration(dur: float, var: float) -> float:
    if var <= 0:
        return dur
    return dur + random.random() * var


def _fail_duration(dur: float, var: float) -> float:
    max_d = _success_duration(dur, var)
    if max_d <= 0:
        return 0.0
    return random.random() * max_d


def _check_fail(rate: int) -> bool:
    if rate <= 0:
        return False
    if rate >= 100:
        return True
    return random.randint(0, 99) < rate