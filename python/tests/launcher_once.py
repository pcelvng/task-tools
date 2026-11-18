"""Run launcher once against file bus paths (for Go cross-lang test)."""

import sys

from task_tools.bus.file import FileConsumer, FileProducer
from task_tools.launcher import Launcher, LauncherOptions
from task_tools.task import Result
from task_tools.worker import Context, Worker


class OnceWorker(Worker):
    def do_task(self, ctx: Context) -> tuple[Result, str]:
        return Result.COMPLETE, "ok"


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit(2)
    inbound, done_path = sys.argv[1], sys.argv[2]
    consumer = FileConsumer(inbound)
    producer = FileProducer()
    opt = LauncherOptions(task_type="echo", done_topic=done_path, lifetime_workers=1)
    launcher = Launcher(lambda _: OnceWorker(), consumer, producer, opt)
    launcher.start()
    sys.exit(0)


if __name__ == "__main__":
    main()