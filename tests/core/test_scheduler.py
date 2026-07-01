"""
Deterministic scheduler tests.

These never start the scheduler's background threads and never call
``time.sleep``. Instead they pin orcha's clock (the ``clock`` fixture) and call
the scheduler's one-shot ``_tick_*`` methods directly, then assert on the
database. Advancing the clock across cron boundaries makes run creation fully
reproducible.
"""
from __future__ import annotations

from datetime import datetime as dt
from datetime import timedelta as td

from orcha.core.tasks import RunItem, TaskItem

SINCE = dt(2020, 1, 1)


def _run_count(task) -> int:
    return len(RunItem.get_all(task=task.task_idk, since=SINCE))


def test_tick_creates_run_on_boundary(scheduler, make_task, clock):
    clock.set(dt(2026, 1, 1, 0, 0, 30))  # 30s past a minute boundary
    task = make_task(idk="sched_task")
    scheduler.all_tasks = [task]

    scheduler._tick_process_schedules()

    runs = RunItem.get_all(task=task.task_idk, schedule=task.schedule_sets[0], since=SINCE)
    assert len(runs) == 1
    assert runs[0].scheduled_time == dt(2026, 1, 1, 0, 0, 0)


def test_tick_is_idempotent_within_the_same_minute(scheduler, make_task, clock):
    clock.set(dt(2026, 1, 1, 0, 0, 30))
    task = make_task(idk="idem_task")
    scheduler.all_tasks = [task]

    scheduler._tick_process_schedules()
    scheduler._tick_process_schedules()  # same clock -> no second run

    assert _run_count(task) == 1


def test_tick_creates_next_run_after_advancing_clock(scheduler, make_task, clock):
    clock.set(dt(2026, 1, 1, 0, 0, 30))
    task = make_task(idk="advance_task")
    scheduler.all_tasks = [task]

    scheduler._tick_process_schedules()
    assert _run_count(task) == 1

    clock.advance(td(minutes=1))  # 00:01:30 -> new boundary crossed
    scheduler._tick_process_schedules()

    runs = RunItem.get_all(task=task.task_idk, since=SINCE)
    assert len(runs) == 2
    assert runs[0].scheduled_time == dt(2026, 1, 1, 0, 1, 0)  # newest first


def test_five_minute_schedule_not_due_within_interval(scheduler, make_task, clock):
    clock.set(dt(2026, 1, 1, 0, 0, 30))
    task = make_task(idk="five_min", crons=("*/5 * * * *",))
    scheduler.all_tasks = [task]

    scheduler._tick_process_schedules()  # first run always due
    assert _run_count(task) == 1

    clock.set(dt(2026, 1, 1, 0, 1, 30))  # still within the 5-minute slot
    scheduler._tick_process_schedules()
    assert _run_count(task) == 1

    clock.set(dt(2026, 1, 1, 0, 5, 30))  # next slot
    scheduler._tick_process_schedules()
    assert _run_count(task) == 2


def test_disabled_task_is_not_scheduled(scheduler, make_task, clock):
    clock.set(dt(2026, 1, 1, 0, 0, 30))
    task = make_task(idk="disabled_sched")
    task.set_status("disabled", "disabled for test")
    scheduler.all_tasks = [task]

    scheduler._tick_process_schedules()
    assert _run_count(task) == 0


def test_stale_task_is_disabled_and_alerts(scheduler, make_task, clock, sent_messages):
    # Create a prior scheduled run in an earlier minute so that a new run is due
    # at the tick time.
    clock.set(dt(2026, 1, 1, 0, 9, 30))
    task = make_task(idk="stale_task")
    task.schedule_run(schedule_by_id="test", schedule=task.schedule_sets[0])

    # Move to the next-but-one minute: a run is due, but the task has not been
    # active since well before the last run -> it should be marked inactive.
    clock.set(dt(2026, 1, 1, 0, 10, 30))
    task.last_active = dt(2026, 1, 1, 0, 4, 0)
    scheduler.all_tasks = [task]

    scheduler._tick_process_schedules()

    reloaded = TaskItem.get("stale_task")
    assert reloaded is not None
    assert reloaded.status == "inactive"
    assert any(channel == "inactive_task" for channel, _ in sent_messages)


def test_fail_historical_marks_old_run_failed(scheduler, make_task, clock, sent_messages):
    clock.set(dt(2026, 1, 1, 12, 0, 0))
    task = make_task(idk="hist_task")
    run = task.schedule_run(schedule_by_id="test", schedule=task.schedule_sets[0])
    scheduler.all_tasks = [task]

    clock.advance(td(hours=7))  # older than the 6h default fail_historical_age
    scheduler._tick_fail_historical()

    reloaded = RunItem.get(run_id=run.run_idk, task=task)
    assert reloaded is not None
    assert reloaded.status == "failed"
    assert reloaded.progress == "complete"
    assert any(channel == "scheduler_historical_run" for channel, _ in sent_messages)
