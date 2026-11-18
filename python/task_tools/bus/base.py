"""Bus interfaces and options."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class BusOptions:
    bus: str = "stdio"
    in_bus: str = ""
    out_bus: str = ""
    in_topic: str = ""
    in_channel: str = ""
    nsqd_hosts: list[str] = field(default_factory=lambda: ["localhost:4150"])
    lookupd_hosts: list[str] = field(default_factory=list)
    pubsub_host: str = ""
    project_id: str = ""
    json_auth: str = ""
    nop_mock: str = ""


class Consumer(ABC):
    @abstractmethod
    def msg(self) -> tuple[bytes, bool, Exception | None]:
        """Return (message, done, error)."""

    @abstractmethod
    def stop(self) -> Exception | None:
        ...

    @abstractmethod
    def info(self) -> dict[str, Any]:
        ...


class Producer(ABC):
    @abstractmethod
    def send(self, topic: str, msg: bytes) -> Exception | None:
        ...

    @abstractmethod
    def stop(self) -> Exception | None:
        ...

    @abstractmethod
    def info(self) -> dict[str, Any]:
        ...


def new_consumer(opt: BusOptions) -> Consumer:
    from task_tools.bus import file as file_bus
    from task_tools.bus import nsq as nsq_bus
    from task_tools.bus import pubsub as pubsub_bus
    from task_tools.bus import stdio as stdio_bus
    from task_tools.bus.nop import new_nop_consumer

    bus_type = opt.in_bus or opt.bus or "stdio"
    if bus_type in ("stdin", "stdio", ""):
        return stdio_bus.StdinConsumer()
    if bus_type == "file":
        return file_bus.FileConsumer(opt.in_topic)
    if bus_type == "nsq":
        return nsq_bus.NSQConsumer(opt.in_topic, opt.in_channel, opt)
    if bus_type == "pubsub":
        return pubsub_bus.PubSubConsumer(opt)
    if bus_type == "nop":
        return new_nop_consumer(opt.nop_mock)
    raise ValueError(f"task bus '{bus_type}' not supported")


def new_producer(opt: BusOptions) -> Producer:
    from task_tools.bus import file as file_bus
    from task_tools.bus import nsq as nsq_bus
    from task_tools.bus import pubsub as pubsub_bus
    from task_tools.bus import stdio as stdio_bus
    from task_tools.bus.nop import new_nop_producer

    bus_type = opt.out_bus or opt.bus or "stdio"
    if bus_type in ("stdout", "stdio", "", "null"):
        if bus_type == "null":
            from task_tools.bus.nop import NullProducer

            return NullProducer()
        return stdio_bus.StdoutProducer()
    if bus_type == "stderr":
        return stdio_bus.StderrProducer()
    if bus_type == "file":
        return file_bus.FileProducer()
    if bus_type == "nsq":
        return nsq_bus.NSQProducer(opt)
    if bus_type == "pubsub":
        return pubsub_bus.PubSubProducer(opt)
    if bus_type == "nop":
        return new_nop_producer(opt.nop_mock)
    raise ValueError(f"task bus '{bus_type}' not supported")