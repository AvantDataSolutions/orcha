"""Task creation, retrieval, status transitions and due-ness logic."""
from __future__ import annotations

import pytest

from orcha.core import tasks
from orcha.core.tasks import TaskItem


def test_no_tasks_on_empty_db():
    assert tasks.TaskItem.get_all() == []


def test_create_task_persists(make_task):
    make_task(idk="task_a")
    fetched = TaskItem.get("task_a")
    assert fetched is not None
    assert fetched.task_idk == "task_a"
    assert len(TaskItem.get_all()) == 1


def test_create_multiple_tasks(make_task):
    make_task(idk="task_a")
    make_task(idk="task_b")
    assert len(TaskItem.get_all()) == 2


@pytest.mark.parametrize("status", ["enabled", "disabled", "inactive", "deleted"])
def test_status_transitions_persist(make_task, status):
    task = make_task(idk="task_status")
    task.set_status(status, "test status change")
    assert task.status == status
    reloaded = TaskItem.get("task_status")
    assert reloaded is not None
    assert reloaded.status == status


def test_create_requires_runner_when_registering(make_task):
    # No TaskRunner is registered in the core suite, so requesting registration
    # must raise rather than silently creating an unrunnable task.
    with pytest.raises(Exception):
        make_task(idk="needs_runner", register=True)


def test_run_due_when_no_runs_exist(make_task):
    task = make_task(idk="due_task")
    is_due, last_run = task.is_run_due_with_last(schedule=task.schedule_sets[0])
    assert is_due is True
    assert last_run is None


def test_not_due_after_scheduling(make_task, clock):
    # Freeze on a boundary so the scheduled run's time equals the current cron
    # slot and a second run is therefore not yet due.
    task = make_task(idk="not_due_task")
    run = task.schedule_run(schedule_by_id="test", schedule=task.schedule_sets[0])
    assert run is not None
    assert run.status == "unstarted"
    assert run.progress == "queued"

    is_due, last_run = task.is_run_due_with_last(schedule=task.schedule_sets[0])
    assert is_due is False
    assert last_run is not None
    assert last_run.run_idk == run.run_idk


def test_disabled_task_does_not_schedule(make_task):
    task = make_task(idk="disabled_task")
    task.set_status("disabled", "disable for test")
    run = task.schedule_run(schedule_by_id="test", schedule=task.schedule_sets[0])
    assert run is None
