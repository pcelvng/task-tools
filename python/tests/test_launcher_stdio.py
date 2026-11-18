import json
import threading
from io import BytesIO

from task_tools.bus.file import FileConsumer, FileProducer
from task_tools.launcher import Launcher, LauncherOptions
from task_tools.task import Result
from task_tools.worker import Context, Worker


class EchoWorker(Worker):
    def do_task(self, ctx: Context) -> tuple[Result, str]:
        return Result.COMPLETE, "ok"


def test_launcher_file_bus(tmp_path) -> None:
    inbound = tmp_path / "in.jsonl"
    done_path = tmp_path / "done"
    task_line = b'{"type":"echo","info":"x"}'
    inbound.write_bytes(task_line + b"\n")

    consumer = FileConsumer(str(inbound))
    producer = FileProducer()
    opt = LauncherOptions(task_type="echo", done_topic=str(done_path), lifetime_workers=1)
    launcher = Launcher(lambda _: EchoWorker(), consumer, producer, opt)

    stop = threading.Event()
    thread = threading.Thread(target=launcher.start, args=(stop,), daemon=True)
    thread.start()

    for _ in range(50):
        if done_path.exists():
            break
        threading.Event().wait(0.05)
    stop.set()
    thread.join(timeout=5)

    lines = done_path.read_text().strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["result"] == "complete"
    assert payload["msg"] == "ok"
    assert "started" in payload
    assert "ended" in payload