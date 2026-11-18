#!/usr/bin/env python3
"""nop worker entrypoint."""

from worker import NopOptions, new_worker

from task_tools.bootstrap import new_worker_app

TASK_TYPE = "nop"
DESCRIPTION = """The nop worker does nothing except listen for tasks and mark the task
as a success or failure at random."""

if __name__ == "__main__":
    app_opt = NopOptions()
    app = (
        new_worker_app(TASK_TYPE, lambda info: new_worker(info, app_opt), app_opt)
        .with_description(DESCRIPTION)
    )
    app.initialize()
    app.run()