"""Worker application bootstrap (mirrors task-tools/bootstrap)."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Protocol

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

from task_tools.bus.base import BusOptions, new_consumer, new_producer
from task_tools.launcher import Launcher, LauncherOptions
from task_tools.worker import NewWorker

_log = logging.getLogger(__name__)


class Validator(Protocol):
    def validate(self) -> None: ...


class NilValidator:
    def validate(self) -> None:
        return None


@dataclass
class LauncherConfig:
    done_topic: str = "done"
    max_in_progress: int = 1
    worker_kill_time: float = 10.0
    lifetime_workers: int = 0
    reject_bad_type: bool = False
    ignore_bad_type: bool = False


@dataclass
class Starter:
    name: str
    new_worker: NewWorker
    app_options: Any = field(default_factory=NilValidator)
    version: str = ""
    description: str = ""
    status_port: int = 0
    bus: BusOptions = field(default_factory=BusOptions)
    launcher: LauncherConfig = field(default_factory=LauncherConfig)
    _launcher: Launcher | None = field(default=None, init=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False)

    def with_version(self, value: str) -> Starter:
        object.__setattr__(self, "version", value)
        return self

    def with_description(self, value: str) -> Starter:
        object.__setattr__(self, "description", value)
        return self

    def initialize(self) -> Starter:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        parser = argparse.ArgumentParser(description=self.description or self.name)
        parser.add_argument("-c", "--config", dest="config", help="path to options file")
        parser.add_argument("-g", action="store_true", help="generate options file")
        parser.add_argument("-show", action="store_true", help="show current options values")
        parser.add_argument("-version", action="store_true", help="show version")
        args, _ = parser.parse_known_args()

        if args.version:
            print(self.version or "unknown")
            sys.exit(0)

        if args.config:
            self._load_toml(args.config)

        if hasattr(self.app_options, "validate"):
            self.app_options.validate()

        if args.g:
            print(self._gen_config())
            sys.exit(0)
        if args.show:
            print(self._show_config())
            sys.exit(0)

        consumer = new_consumer(self.bus)
        producer = new_producer(self.bus)
        opt = LauncherOptions(
            max_in_progress=self.launcher.max_in_progress,
            worker_kill_time=self.launcher.worker_kill_time,
            lifetime_workers=self.launcher.lifetime_workers,
            done_topic=self.launcher.done_topic,
            task_type=self.name,
            reject_bad_type=self.launcher.reject_bad_type,
            ignore_bad_type=self.launcher.ignore_bad_type,
        )
        self._launcher = Launcher(self.new_worker, consumer, producer, opt)
        return self

    def _load_toml(self, path: str) -> None:
        data = tomllib.loads(Path(path).read_text())
        self.status_port = int(data.get("status_port", self.status_port))
        bus = data.get("bus", {})
        if bus:
            self.bus = BusOptions(
                bus=bus.get("bus", self.bus.bus),
                in_bus=bus.get("in_bus", ""),
                out_bus=bus.get("out_bus", ""),
                in_topic=bus.get("in_topic", self.bus.in_topic or self.name),
                in_channel=bus.get("in_channel", self.bus.in_channel or self.name),
                nsqd_hosts=list(bus.get("nsqd_hosts", self.bus.nsqd_hosts)),
                lookupd_hosts=list(bus.get("lookupd_hosts", [])),
                pubsub_host=bus.get("pubsub_host", ""),
                project_id=bus.get("project_id", ""),
                json_auth=bus.get("json_auth", ""),
            )
        launch = data.get("launcher", {})
        if launch:
            self.launcher = LauncherConfig(
                done_topic=launch.get("done_topic", self.launcher.done_topic),
                max_in_progress=int(launch.get("max_in_progress", self.launcher.max_in_progress)),
                worker_kill_time=_parse_duration(launch.get("worker_kill_time", "10s")),
                lifetime_workers=int(launch.get("lifetime_workers", 0)),
                reject_bad_type=bool(launch.get("reject_bad_type", False)),
                ignore_bad_type=bool(launch.get("ignore_bad_type", False)),
            )
        app_section = {k: v for k, v in data.items() if k not in ("status_port", "bus", "launcher")}
        if app_section and hasattr(self.app_options, "__dict__"):
            for k, v in app_section.items():
                if hasattr(self.app_options, k):
                    setattr(self.app_options, k, v)

    def _gen_config(self) -> str:
        lines = [
            f"status_port = {self.status_port}",
            "",
            "# application options",
        ]
        if hasattr(self.app_options, "__dict__"):
            for k, v in self.app_options.__dict__.items():
                if not k.startswith("_"):
                    lines.append(f"{k} = {json.dumps(v)}")
        lines.extend(
            [
                "",
                "# task message bus (nsq, pubsub, file, stdio)",
                "[bus]",
                f'  bus = "{self.bus.bus}"',
                f'  #in_topic = "{self.bus.in_topic or self.name}"',
                f'  #in_channel = "{self.bus.in_channel or self.name}"',
                "",
                "[launcher]",
                f'  done_topic = "{self.launcher.done_topic}"',
                f"  max_in_progress = {self.launcher.max_in_progress}",
            ]
        )
        return "\n".join(lines) + "\n"

    def _show_config(self) -> str:
        return json.dumps(
            {
                "status_port": self.status_port,
                "app_options": getattr(self.app_options, "__dict__", {}),
                "bus": self.bus.__dict__,
                "launcher": self.launcher.__dict__,
            },
            indent=2,
        )

    def run(self) -> None:
        if self._launcher is None:
            raise RuntimeError("call initialize() before run()")

        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGQUIT):
            signal.signal(sig, lambda *_: self._stop.set())

        if self.status_port:
            self._start_status_server()

        _log.info("starting %s", self.name)
        _log.info("listening for %s tasks on '%s'", self.bus.bus, self.bus.in_topic)
        self._launcher.start(self._stop)
        err = self._launcher.err()
        if err:
            _log.error("launcher error: %s", err)
            sys.exit(1)
        sys.exit(0)

    def _start_status_server(self) -> None:
        starter = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                assert starter._launcher is not None
                stats = starter._launcher.stats()
                body = {
                    "app_name": starter.name,
                    "version": starter.version,
                    "launcher": {
                        "runtime": stats.runtime,
                        "tasks_consumed": stats.tasks_consumed,
                        "tasks_running": stats.tasks_running,
                        "mean_tasktime": stats.mean_tasktime,
                        "producer": stats.producer,
                        "consumer": stats.consumer,
                    },
                }
                data = json.dumps(body, indent=2).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.endheaders()
                self.wfile.write(data)

            def log_message(self, format: str, *args: Any) -> None:
                return

        server = HTTPServer(("", self.status_port), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        _log.info("starting http status server on port %d", self.status_port)

    def new_consumer(self) -> Any:
        return new_consumer(self.bus)

    def new_producer(self) -> Any:
        return new_producer(self.bus)


def new_worker_app(tsk_type: str, new_worker: NewWorker, options: Any | None = None) -> Starter:
    if options is None:
        options = NilValidator()
    bus = BusOptions(
        bus="stdio",
        in_topic=tsk_type,
        in_channel=tsk_type,
    )
    return Starter(name=tsk_type, new_worker=new_worker, app_options=options, bus=bus)


def _parse_duration(value: str | float | int) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    value = str(value).strip()
    if value.endswith("s") and value[:-1].replace(".", "", 1).isdigit():
        return float(value[:-1])
    if value.endswith("m"):
        return float(value[:-1]) * 60
    return float(value)