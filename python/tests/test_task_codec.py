from pathlib import Path

import pytest

from task_tools.task import Result, Task, new_from_bytes

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.mark.parametrize(
    "name",
    ["minimal", "full_in", "meta_special", "completed"],
)
def test_golden_roundtrip(name: str) -> None:
    raw = (FIXTURES / f"{name}.json").read_text().strip()
    tsk = new_from_bytes(raw)
    assert tsk.json_string() == raw


def test_start_end_field_order() -> None:
    tsk = Task.new("nop", "info")
    tsk.start()
    tsk.end(Result.COMPLETE, "ok")
    data = tsk.json_string()
    assert data.index('"type"') < data.index('"info"') < data.index('"result"')
    assert '"result":"complete"' in data


def test_merge_meta_worker_wins() -> None:
    tsk = Task(type="t", info="i", meta="a=1")
    tsk.merge_meta({"a": ["2"], "b": ["x"]})
    assert "a=2" in tsk.meta
    assert "b=x" in tsk.meta