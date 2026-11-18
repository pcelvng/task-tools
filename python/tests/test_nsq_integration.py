import os
import socket
import subprocess
import threading
import time

import pytest

from task_tools.bus.base import BusOptions
from task_tools.bus.nsq import NSQConsumer, NSQProducer


def _nsqd_available() -> bool:
    host = os.environ.get("NSQD_TCP", "127.0.0.1:4150")
    h, p = host.rsplit(":", 1)
    try:
        s = socket.create_connection((h, int(p)), timeout=0.5)
        s.close()
        return True
    except OSError:
        return False


@pytest.mark.skipif(not _nsqd_available(), reason="nsqd not reachable")
def test_nsq_pub_sub_roundtrip() -> None:
    topic = "py_test_topic"
    channel = "py_test_channel"
    opt = BusOptions(bus="nsq", nsqd_hosts=[os.environ.get("NSQD_TCP", "127.0.0.1:4150")])
    consumer = NSQConsumer(topic, channel, opt)
    producer = NSQProducer(opt)
    payload = b'{"type":"t","info":"i"}'

    def publish() -> None:
        time.sleep(0.5)
        err = producer.send(topic, payload)
        assert err is None

    threading.Thread(target=publish, daemon=True).start()
    msg, done, err = consumer.msg()
    assert err is None
    assert not done
    assert msg == payload
    consumer.stop()
    producer.stop()