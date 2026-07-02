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

import pytest

from orcha.core.scheduler import OrchaSchedulerConfig, Scheduler
from orcha.core.tasks import RunItem, TaskItem

SINCE = dt(2020, 1, 1)


def _stop(sched: Scheduler) -> None:
    """Stop a directly-constructed scheduler's background thread(s)."""
    if sched.last_active_thread is not None:
        sched.last_active_thread.stop()
    sched.stop()


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


def test_deprecated_fail_unstarted_runs_warns_and_still_applies():
    # The deprecated kwarg must warn (not raise) and its value must still take
    # effect, overriding the config default of True.
    with pytest.warns(DeprecationWarning, match="fail_unstarted_runs"):
        sched = Scheduler(config=OrchaSchedulerConfig(), fail_unstarted_runs=False)
    try:
        assert sched.fail_unstarted_runs is False
    finally:
        _stop(sched)


def test_deprecated_disable_stale_tasks_warns_and_still_applies():
    with pytest.warns(DeprecationWarning, match="disable_stale_tasks"):
        sched = Scheduler(config=OrchaSchedulerConfig(), disable_stale_tasks=False)
    try:
        assert sched.disable_stale_tasks is False
    finally:
        _stop(sched)


def test_no_deprecation_warning_on_supported_config_path(recwarn):
    # The supported path (config only) must not emit the deprecation warning.
    sched = Scheduler(config=OrchaSchedulerConfig())
    try:
        assert not [w for w in recwarn.list if issubclass(w.category, DeprecationWarning)]
    finally:
        _stop(sched)


def test_scheduler_identity_is_configurable():
    # A scheduler records its liveness under its own configured id, not a
    # hardcoded 'main', so multiple schedulers don't clobber each other's row.
    sched = Scheduler(config=OrchaSchedulerConfig(scheduler_idk="sched_b"))
    try:
        assert sched.scheduler_idk == "sched_b"
        assert Scheduler.get_loaded_at("sched_b") is not None
        assert Scheduler.get_loaded_at("main") is None
    finally:
        _stop(sched)


def test_supersedes_prior_pending_run_and_alerts(scheduler, make_task, clock, sent_messages):
    # A schedule that never gets picked up must keep at most one pending run:
    # when the next slot is due, the prior pending run is failed (superseded).
    clock.set(dt(2026, 1, 1, 0, 0, 30))
    task = make_task(idk="supersede_task")
    scheduler.all_tasks = [task]

    scheduler._tick_process_schedules()  # slot 00:00 -> one pending run
    first = RunItem.get_all(task=task.task_idk, since=SINCE)
    assert len(first) == 1
    first_run = first[0]
    assert first_run.status == "unstarted"

    # Next slot due while the prior run is still pending.
    clock.set(dt(2026, 1, 1, 0, 1, 30))
    scheduler._tick_process_schedules()

    runs = RunItem.get_all(task=task.task_idk, since=SINCE)
    assert len(runs) == 2  # the new run was created

    superseded = RunItem.get(run_id=first_run.run_idk, task=task)
    assert superseded is not None
    assert superseded.status == "failed"
    assert superseded.progress == "complete"
    assert superseded.output.get("message") == "superseded by newer scheduled run"
    # The supersession emits a run_failed alert so it counts toward
    # FailedRunsMonitor's disable-after-N path.
    assert any(ch == "run_failed" for ch, _ in sent_messages)


def test_only_one_pending_run_accumulates_over_many_slots(scheduler, make_task, clock):
    # Ticking across several slots without any runner never leaves more than one
    # pending run at a time (each new slot supersedes the previous pending one).
    clock.set(dt(2026, 1, 1, 0, 0, 30))
    task = make_task(idk="accumulate_task")
    scheduler.all_tasks = [task]

    for minute in range(4):
        clock.set(dt(2026, 1, 1, 0, minute, 30))
        scheduler._tick_process_schedules()
        pending = RunItem.get_all_queued(task=task.task_idk)
        assert len(pending) == 1
