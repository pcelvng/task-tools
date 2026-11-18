"""Task launcher: consumer loop, worker lifecycle, done publish."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from task_tools.bus.base import Consumer, Producer
from task_tools.task import Result, Task, new_from_bytes
from task_tools.worker import Context, NewWorker, Worker

TRUNC_TIME = 0.01  # 10ms, matches Go truncTime
DEFAULT_WORKER_KILL_TIME = 10.0
DEFAULT_DONE_TOPIC = "done"
DEFAULT_MAX_IN_PROGRESS = 1


@dataclass
class LauncherOptions:
    max_in_progress: int = DEFAULT_MAX_IN_PROGRESS
    worker_kill_time: float = DEFAULT_WORKER_KILL_TIME
    lifetime_workers: int = 0
    done_topic: str = DEFAULT_DONE_TOPIC
    task_type: str = ""
    reject_bad_type: bool = False
    ignore_bad_type: bool = False
    logger: logging.Logger | None = None


@dataclass
class LauncherStats:
    runtime: str = ""
    tasks_consumed: int = 0
    tasks_running: int = 0
    mean_tasktime: str = ""
    producer: dict[str, Any] = field(default_factory=dict)
    consumer: dict[str, Any] = field(default_factory=dict)


class Launcher:
    def __init__(
        self,
        new_worker: NewWorker,
        consumer: Consumer,
        producer: Producer,
        opt: LauncherOptions | None = None,
    ) -> None:
        self._new_worker = new_worker
        self._consumer = consumer
        self._producer = producer
        self._opt = opt or LauncherOptions()
        self._log = self._opt.logger or logging.getLogger("task_tools.launcher")

        self._max_in_progress = max(1, self._opt.max_in_progress)
        if self._max_in_progress > 1:
            self._log.warning(
                "max_in_progress=%d: Python threads do not parallelize CPU work; "
                "prefer multiple processes for CPU-bound tasks",
                self._max_in_progress,
            )

        self._remaining = self._opt.lifetime_workers
        self._slots = threading.Semaphore(self._max_in_progress)
        self._init_time = datetime.now(timezone.utc)
        self._tasks_consumed = 0
        self._tasks_running = 0
        self._task_runtime_sum = 0.0
        self._lock = threading.Lock()
        self._close_err: Exception | None = None
        self._type_handling = ""
        if self._opt.ignore_bad_type:
            self._type_handling = "ignore"
        if self._opt.reject_bad_type:
            self._type_handling = "reject"
        if self._type_handling and not self._opt.task_type:
            self._log.warning(
                "NO WORKERS WILL BE LAUNCHED! task type handling is set to '%s' "
                "but no task type is provided",
                self._type_handling,
            )
        self._pool: ThreadPoolExecutor | None = None
        if self._max_in_progress > 1:
            self._pool = ThreadPoolExecutor(max_workers=self._max_in_progress, thread_name_prefix="worker")
        self._in_flight = 0
        self._in_flight_lock = threading.Lock()

    def _inc_in_flight(self) -> None:
        with self._in_flight_lock:
            self._in_flight += 1

    def _dec_in_flight(self) -> None:
        with self._in_flight_lock:
            self._in_flight -= 1

    def _wait_in_flight(self) -> None:
        while True:
            with self._in_flight_lock:
                if self._in_flight <= 0:
                    return
            time.sleep(0.01)

    def stats(self) -> LauncherStats:
        finished = self._tasks_consumed - self._tasks_running
        mean = ""
        if finished > 0:
            mean = f"{(self._task_runtime_sum / finished) * TRUNC_TIME:.3f}s"
        return LauncherStats(
            runtime=str(datetime.now(timezone.utc) - self._init_time),
            tasks_consumed=self._tasks_consumed,
            tasks_running=self._tasks_running,
            mean_tasktime=mean,
            producer=self._producer.info(),
            consumer=self._consumer.info(),
        )

    def start(self, stop_event: threading.Event | None = None) -> None:
        """Blocking task loop until shutdown."""
        stop = stop_event or threading.Event()
        local_shutdown = threading.Event()

        def on_stop() -> None:
            local_shutdown.set()

        if stop_event is not None:
            threading.Thread(target=lambda: (stop.wait(), on_stop()), daemon=True).start()

        try:
            while not local_shutdown.is_set():
                acquired = self._slots.acquire(timeout=0.1)
                if not acquired:
                    continue

                with self._lock:
                    if self._remaining != 0:
                        if self._remaining > 0:
                            self._remaining -= 1
                        if self._remaining == 0:
                            on_stop()

                self._inc_in_flight()
                if self._pool:
                    self._pool.submit(self._next, local_shutdown)
                else:
                    threading.Thread(target=self._next, args=(local_shutdown,), daemon=True).start()
        finally:
            err = self._consumer.stop()
            if err:
                self._close_err = err
            self._wait_in_flight()
            err = self._producer.stop()
            if err and not self._close_err:
                self._close_err = err
            if self._pool:
                self._pool.shutdown(wait=True)

    def _next(self, shutdown: threading.Event) -> None:
        try:
            msg, done, err = self._consumer.msg()
            if done:
                shutdown.set()
            if err:
                self._log.error("%s", err)
                self._give_back_slot(shutdown)
                return
            if not msg:
                self._give_back_slot(shutdown)
                return
            try:
                tsk = new_from_bytes(msg)
            except Exception as exc:
                self._log.error("%s", exc)
                self._give_back_slot(shutdown)
                return

            self._tasks_consumed += 1
            self._tasks_running += 1
            try:
                self._do_launch(tsk, shutdown)
            finally:
                self._tasks_running -= 1
        finally:
            self._dec_in_flight()

    def _give_back_slot(self, shutdown: threading.Event) -> None:
        if not shutdown.is_set():
            self._slots.release()

    def _bad_type(self, tsk: Task) -> bool:
        return bool(self._opt.task_type and tsk.type != self._opt.task_type)

    def _do_launch(self, tsk: Task, shutdown: threading.Event) -> None:
        try:
            tsk.start()
            send_done = True

            if self._type_handling == "reject" and self._bad_type(tsk):
                tsk.end(
                    Result.ERROR,
                    f"unexpected task type '{tsk.type}' wanting '{self._opt.task_type}'",
                )
            elif self._type_handling == "ignore" and self._bad_type(tsk):
                send_done = False
            else:
                worker = self._new_worker(tsk.info)
                parent_cancel = threading.Event()
                done_chan = threading.Event()
                result_holder: list[tuple[Result, str]] = []

                def run_worker() -> None:
                    ctx = Context(parent_cancel)
                    result_holder.append(worker.do_task(ctx))
                    meta = worker.get_meta()
                    if meta:
                        tsk.merge_meta(meta)
                    done_chan.set()

                worker_thread = threading.Thread(target=run_worker, daemon=True)
                worker_thread.start()

                if shutdown.is_set():
                    parent_cancel.set()

                if done_chan.wait(timeout=None if not shutdown.is_set() else self._opt.worker_kill_time):
                    if result_holder:
                        r, m = result_holder[0]
                        tsk.end(r, m)
                        if tsk._started and tsk._ended:
                            exec_time = (tsk._ended - tsk._started).total_seconds() / TRUNC_TIME
                            self._task_runtime_sum += exec_time
                else:
                    parent_cancel.set()
                    if done_chan.wait(timeout=self._opt.worker_kill_time):
                        if result_holder:
                            r, m = result_holder[0]
                            tsk.end(r, m)
                    else:
                        tsk.end(
                            Result.ERROR,
                            f"worker forced to close; waited '{self._opt.worker_kill_time}s'",
                        )

            if send_done:
                self._producer.send(self._opt.done_topic, tsk.json_bytes())
        finally:
            self._give_back_slot(shutdown)

    def err(self) -> Exception | None:
        return self._close_err