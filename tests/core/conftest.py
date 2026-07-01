"""
Fixtures for the ``core`` suite: the tests that run against a fully initialised
Orcha (tasks, runs, scheduler, runner).

Design notes:
- ``orcha`` is initialised once per session (it configures process-global state
  and cannot be un-initialised); migrations build the schema.
- Every test starts from a truncated database (``clean_db``, autouse).
- The scheduler is created per-test with its background threads stopped, so
  tests drive it by calling its ``_tick_*`` methods directly — deterministic,
  no ``time.sleep``, no thread races.
- ``Producer.send_message`` is patched to record messages instead of hitting a
  (non-existent in tests) mqueue broker, so tests can assert what was emitted.
"""
from __future__ import annotations

from typing import Callable

import pytest

import orcha
from orcha.core import initialise, tasks
from orcha.core.scheduler import OrchaSchedulerConfig, Scheduler
from orcha.core.tasks import RunItem, ScheduleSet, TaskItem

from helpers import (
    ORCHA_DB,
    ORCHA_PASSWORD,
    ORCHA_SERVER,
    ORCHA_USER,
    empty_database,
    noop_task,
)

NoopFunc = Callable[[TaskItem, RunItem, dict], None]


@pytest.fixture(scope="session", autouse=True)
def _initialised():
    """Initialise orcha once for the whole session (runs migrations)."""
    initialise(
        orcha_user=ORCHA_USER,
        orcha_pass=ORCHA_PASSWORD,
        orcha_server=ORCHA_SERVER,
        orcha_db=ORCHA_DB,
        application_name="orcha_tests",
    )
    tasks.confirm_initialised()
    yield


@pytest.fixture(autouse=True)
def clean_db(_initialised):
    """Truncate all orcha tables before each test."""
    empty_database()
    yield


@pytest.fixture
def sent_messages() -> list[tuple[str, object]]:
    """Collector for messages emitted via a patched Producer (see below)."""
    return []


@pytest.fixture(autouse=True)
def patch_producer(monkeypatch, sent_messages):
    """
    Replace ``Producer.send_message`` with a recorder. Tests have no mqueue
    broker configured, so the real method would raise; recording lets tests both
    run and assert on emitted alerts (inactive task, historical run, etc.).
    """
    from orcha.utils import mqueue

    def _record(self, channel, message):  # noqa: ANN001
        sent_messages.append((channel.name, message))
        return "recorded"

    monkeypatch.setattr(mqueue.Producer, "send_message", _record)


@pytest.fixture
def scheduler():
    """
    A Scheduler with no running background threads.

    ``Scheduler.__init__`` starts a single 'last active' ManagedThread; we stop
    it so the fixture is fully single-threaded. ``start()`` is never called, so
    the process/refresh/prune/fail loops never run — tests invoke the
    corresponding ``_tick_*`` methods directly for deterministic behaviour.
    """
    sched = Scheduler(config=OrchaSchedulerConfig())
    if sched.last_active_thread is not None:
        sched.last_active_thread.stop()
    yield sched
    sched.stop()


@pytest.fixture
def make_task():
    """
    Factory for test tasks. Defaults to a single every-minute schedule and does
    not register with the runner (most tests drive scheduling/execution
    explicitly and don't need the global runner registration).
    """

    def _make(
        idk: str = "test_task",
        crons: tuple[str, ...] = ("* * * * *",),
        configs: dict[str, dict] | None = None,
        func: NoopFunc | None = None,
        register: bool = False,
        thread_group: str = "test_group",
    ) -> TaskItem:
        configs = configs or {}
        s_sets = [ScheduleSet(cron, configs.get(cron, {})) for cron in crons]
        return TaskItem.create(
            task_idk=idk,
            name=idk,
            description=f"test task {idk}",
            schedule_sets=s_sets,
            task_function=func or noop_task,
            thread_group=thread_group,
            register_with_runner=register,
        )

    return _make
