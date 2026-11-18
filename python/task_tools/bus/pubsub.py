"""Google Cloud Pub/Sub bus."""

from __future__ import annotations

import json
import os
import threading
from typing import Any

from task_tools.bus.base import BusOptions

try:
    from google.cloud import pubsub_v1
    from google.oauth2 import service_account
except ImportError:  # pragma: no cover - optional extra
    pubsub_v1 = None  # type: ignore[assignment]
    service_account = None  # type: ignore[assignment]


def _client(opt: BusOptions):
    if pubsub_v1 is None:
        raise ImportError("google-cloud-pubsub is required; pip install task-tools[pubsub]")
    if opt.pubsub_host:
        os.environ.setdefault("PUBSUB_EMULATOR_HOST", opt.pubsub_host)
    if opt.json_auth:
        creds = service_account.Credentials.from_service_account_info(json.loads(opt.json_auth))
        return pubsub_v1.PublisherClient(credentials=creds), pubsub_v1.SubscriberClient(credentials=creds)
    return pubsub_v1.PublisherClient(), pubsub_v1.SubscriberClient()


class PubSubProducer:
    def __init__(self, opt: BusOptions) -> None:
        self._opt = opt
        self._publisher, _ = _client(opt)
        self._info: dict[str, Any] = {"bus": "pubsub", "sent": {}}
        self._topic_path = self._publisher.topic_path(opt.project_id, opt.in_topic)

    def send(self, topic: str, msg: bytes) -> Exception | None:
        path = self._publisher.topic_path(self._opt.project_id, topic)
        try:
            future = self._publisher.publish(path, msg)
            future.result(timeout=30)
            sent = self._info.setdefault("sent", {})
            sent[topic] = int(sent.get(topic, 0)) + 1
            return None
        except Exception as exc:
            return exc

    def stop(self) -> Exception | None:
        return None

    def info(self) -> dict[str, Any]:
        return dict(self._info)


class PubSubConsumer:
    def __init__(self, opt: BusOptions) -> None:
        if not opt.project_id:
            raise ValueError("project_id required for pubsub bus")
        self._opt = opt
        self._publisher, self._subscriber = _client(opt)
        self._stopped = threading.Event()
        import queue

        self._msg_chan: queue.Queue[bytes] = queue.Queue()
        self._info: dict[str, Any] = {"bus": "pubsub", "topic": opt.in_topic, "received": 0}

        topic_path = self._publisher.topic_path(opt.project_id, opt.in_topic)
        try:
            self._publisher.get_topic(request={"topic": topic_path})
        except Exception:
            self._publisher.create_topic(request={"name": topic_path})

        sub_path = self._subscriber.subscription_path(opt.project_id, opt.in_channel or opt.in_topic)
        try:
            self._subscriber.get_subscription(request={"subscription": sub_path})
        except Exception:
            self._subscriber.create_subscription(
                request={
                    "name": sub_path,
                    "topic": topic_path,
                    "ack_deadline_seconds": 10,
                }
            )
        self._subscription = sub_path
        self._streaming = self._subscriber.subscribe(
            sub_path,
            callback=self._on_message,
            flow_control=pubsub_v1.types.FlowControl(max_messages=1),
        )

    def _on_message(self, message: pubsub_v1.subscriber.message.Message) -> None:
        if self._stopped.is_set():
            message.nack()
            return
        self._msg_chan.put(message.data)
        message.ack()

    def msg(self) -> tuple[bytes, bool, Exception | None]:
        if self._stopped.is_set():
            return b"", True, None
        while not self._stopped.is_set():
            import queue as queue_mod

            try:
                data = self._msg_chan.get(timeout=0.2)
                self._info["received"] = int(self._info["received"]) + 1
                return data, False, None
            except queue_mod.Empty:
                continue
        return b"", True, None

    def stop(self) -> Exception | None:
        self._stopped.set()
        try:
            self._streaming.cancel()
        except Exception:
            pass
        return None

    def info(self) -> dict[str, Any]:
        return dict(self._info)