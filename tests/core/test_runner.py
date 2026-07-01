"""
Task runner execution tests.

The runner is exercised synchronously: a ``ThreadHandler`` is built directly and
``process_all_tasks()`` runs each queued run inline and returns once complete —
no polling, no ``time.sleep`` waiting for a background interval. (A single run
still takes a few seconds because the runner's active-time helper ticks every
5s, but the result is deterministic.)

Task functions are defined at module level (column 0): orcha's
``get_config_keys`` parses their source with no dedent, so they must not be
indented — which is how real apps define task functions anyway.
"""
from __future__ import annotations

from datetime import datetime as dt

import pandas as pd

from orcha.core.module_base import PythonEntity, PythonSource, TransformBase
from orcha.core.task_runner import ThreadHandler
from orcha.core.tasks import RunItem

SINCE = dt(2020, 1, 1)


# --- module-level task functions ---------------------------------------------

def output_hello(task_item, run_item, cfg):
    run_item.set_output({"data": "hello"})


def raise_boom(task_item, run_item, cfg):
    raise RuntimeError("boom")


def module_source_task(task_item, run_item, cfg):
    entity = PythonEntity(
        module_idk="test_entity", description="e", user_name="u", password="p",
    )
    source = PythonSource(
        module_idk="test_source", description="s", data_entity=entity,
        function=lambda x: pd.DataFrame({"n": [1, 2, 3]}),
    )
    data = source.get()
    run_item.set_output({"data": data.to_dict(orient="records")})


def transform_task(task_item, run_item, cfg):
    entity = PythonEntity(
        module_idk="test_entity", description="e", user_name="u", password="p",
    )
    source = PythonSource(
        module_idk="test_source", description="s", data_entity=entity,
        function=lambda x: pd.DataFrame({"v": [" 1", "2 ", " 3 "]}),
    )
    transform = TransformBase[pd.DataFrame](
        module_idk="test_transform", description="t",
        transform_func=lambda x: x.map(lambda v: v.strip() if isinstance(v, str) else v),
        create_inputs=pd.DataFrame,
    )
    data = source.get()
    data = transform.transform(transform.create_inputs(data=data))
    run_item.set_output({"data": data.to_dict(orient="records")})


# --- helpers -----------------------------------------------------------------

def _process(task) -> RunItem:
    """Queue a run for the task, run the handler once, return the reloaded run."""
    task.schedule_run(schedule_by_id="test", schedule=task.schedule_sets[0])
    handler = ThreadHandler(task.thread_group)
    handler.add_task(task)
    handler.process_all_tasks()
    return RunItem.get_all(task=task.task_idk, schedule=task.schedule_sets[0], since=SINCE)[0]


# --- tests -------------------------------------------------------------------

def test_runner_completes_run_successfully(make_task, clock):
    task = make_task(idk="runner_ok")
    run = _process(task)
    assert run.status == "success"
    assert run.progress == "complete"


def test_runner_captures_task_output(make_task, clock):
    task = make_task(idk="runner_output", func=output_hello)
    run = _process(task)
    assert run.status == "success"
    assert run.output is not None
    assert run.output["data"] == "hello"


def test_runner_marks_failure_on_exception(make_task, clock):
    task = make_task(idk="runner_fail", func=raise_boom)
    run = _process(task)
    assert run.status == "failed"
    assert run.progress == "complete"


def test_runner_runs_module_source(make_task, clock):
    task = make_task(idk="runner_module", func=module_source_task)
    run = _process(task)

    assert run.status == "success"
    assert run.output is not None
    assert run.output["data"] == [{"n": 1}, {"n": 2}, {"n": 3}]
    # The module run is timed and recorded.
    assert len(run.output["run_times"]) == 1
    run_time = run.output["run_times"][0]
    for key in ("module_idk", "start_time_posix", "end_time_posix", "duration_seconds"):
        assert key in run_time


def test_runner_runs_transform(make_task, clock):
    task = make_task(idk="runner_transform", func=transform_task)
    run = _process(task)

    assert run.status == "success"
    assert run.output is not None
    assert run.output["data"] == [{"v": "1"}, {"v": "2"}, {"v": "3"}]
    # Source + transform are both timed.
    assert len(run.output["run_times"]) == 2
