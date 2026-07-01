"""Run lifecycle: creation, status/progress transitions, queries and pruning."""
from __future__ import annotations

from datetime import datetime as dt
from datetime import timedelta as td

import pytest

from orcha.core.tasks import RunItem, ScheduleSet

SINCE = dt(2020, 1, 1)


def _schedule(task, schedule=None):
    return task.schedule_run(
        schedule_by_id="test",
        schedule=schedule or task.schedule_sets[0],
    )


def test_schedule_run_creates_queued_run(make_task, clock):
    task = make_task(idk="run_task")
    run = _schedule(task)

    runs = RunItem.get_all(task=task.task_idk, schedule=task.schedule_sets[0], since=SINCE)
    assert len(runs) == 1
    assert runs[0].run_idk == run.run_idk
    assert runs[0].status == "unstarted"
    assert runs[0].progress == "queued"


def test_run_status_forward_transitions(make_task, clock):
    task = make_task(idk="status_task")
    run = _schedule(task)

    run.set_status("pending")
    assert run.status == "pending"
    run.set_status("success", output={"result": "ok"})
    assert run.status == "success"
    assert run.output == {"result": "ok"}


def test_run_status_backwards_raises(make_task, clock):
    task = make_task(idk="backwards_task")
    run = _schedule(task)
    run.set_status("failed")
    # success is 'earlier' than failed in the status order -> must raise
    with pytest.raises(Exception):
        run.set_status("success")


def test_run_progress_transitions(make_task, clock):
    task = make_task(idk="progress_task")
    run = _schedule(task)
    assert run.progress == "queued"
    run.set_progress("running")
    assert run.progress == "running"
    run.set_progress("complete")
    assert run.progress == "complete"


def test_get_queued_and_running(make_task, clock):
    task = make_task(idk="queued_task")
    run = _schedule(task)

    queued = RunItem.get_all_queued(task=task.task_idk)
    assert [r.run_idk for r in queued] == [run.run_idk]
    assert RunItem.get_running_runs(task=task.task_idk) == []

    run.set_status("pending")
    run.set_progress("running")

    running = RunItem.get_running_runs(task=task.task_idk)
    assert [r.run_idk for r in running] == [run.run_idk]
    # No longer queued once running.
    assert RunItem.get_all_queued(task=task.task_idk) == []


def test_runs_are_associated_with_correct_schedule(make_task, clock):
    task = make_task(
        idk="assoc_task",
        crons=("* * * * *", "*/5 * * * *"),
        configs={"* * * * *": {"which": "1min"}, "*/5 * * * *": {"which": "5min"}},
    )
    s1, s5 = task.schedule_sets

    run_1 = _schedule(task, s1)
    run_5 = _schedule(task, s5)

    runs_1 = RunItem.get_all(task=task.task_idk, schedule=s1, since=SINCE)
    runs_5 = RunItem.get_all(task=task.task_idk, schedule=s5, since=SINCE)
    assert [r.run_idk for r in runs_1] == [run_1.run_idk]
    assert [r.run_idk for r in runs_5] == [run_5.run_idk]

    all_runs = RunItem.get_all(task=task.task_idk, since=SINCE)
    assert {r.run_idk for r in all_runs} == {run_1.run_idk, run_5.run_idk}


def test_get_all_rejects_foreign_schedule(make_task, clock):
    task = make_task(idk="foreign_task")
    # A schedule set with no id / not attached to this task must be rejected.
    stray = ScheduleSet("* * * * *", {})
    with pytest.raises(Exception):
        RunItem.get_all(task=task.task_idk, schedule=stray, since=SINCE)


def test_prune_runs_removes_all_and_makes_due(make_task, clock):
    task = make_task(idk="prune_task")
    _schedule(task)
    _schedule(task)
    assert len(RunItem.get_all(task=task.task_idk, since=SINCE)) == 2

    task.prune_runs(max_age=td(seconds=0))
    assert RunItem.get_all(task=task.task_idk, since=SINCE) == []

    # With no runs left, a run is due again.
    is_due, last_run = task.is_run_due_with_last(schedule=task.schedule_sets[0])
    assert is_due is True
    assert last_run is None
