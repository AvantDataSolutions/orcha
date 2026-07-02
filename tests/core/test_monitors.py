"""
Monitor construction: each monitor must own its own mutable collections.

These are the regression tests for the shared-mutable-default bug (a default
``tasks=set()`` / ``schedulers=[]`` on ``__init__`` is created once at import
time and would be shared by every monitor built without an explicit value).

Constructing a monitor normally reaches ``MonitorBase.__init__``, which
registers the monitor as an mqueue consumer and pings a (non-existent in tests)
broker over HTTP. That is unrelated to what we are asserting here, so both calls
are patched out.
"""
from __future__ import annotations

import pytest

from orcha.core.scheduler import SchedulerMonitor
from orcha.core.tasks import FailedRunsMonitor


@pytest.fixture(autouse=True)
def _no_broker(monkeypatch):
    """Neutralise MonitorBase's consumer registration / broker ping."""
    from orcha.utils import mqueue

    monkeypatch.setattr(mqueue.Consumer, "register_consumer", lambda *a, **k: None)
    monkeypatch.setattr(mqueue.Consumer, "run", lambda *a, **k: None)


def test_task_monitors_do_not_share_tasks_set():
    m1 = FailedRunsMonitor(monitor_Name="m1", alert=lambda s: None)
    m2 = FailedRunsMonitor(monitor_Name="m2", alert=lambda s: None)

    # Distinct objects, both empty to begin with.
    assert m1.tasks is not m2.tasks
    assert m1.tasks == set()
    assert m2.tasks == set()

    # Mutating one must not leak into the other (the original bug).
    # add_task is typed for TaskItem but only requires a hashable at runtime.
    m1.add_task("task-x")  # type: ignore[arg-type]
    assert m1.tasks == {"task-x"}
    assert m2.tasks == set()


def test_explicit_tasks_set_is_respected():
    shared = {"pre-seeded"}
    m = FailedRunsMonitor(monitor_Name="m3", alert=lambda s: None)
    # The base class still accepts an explicit set when one is provided.
    m.tasks = shared
    m.add_task("another")  # type: ignore[arg-type]
    assert m.tasks == {"pre-seeded", "another"}


def test_scheduler_monitors_do_not_share_schedulers_list():
    s1 = SchedulerMonitor(alert=lambda s: None)
    s2 = SchedulerMonitor(alert=lambda s: None)

    assert s1.schedulers is not s2.schedulers
    assert s1.schedulers == []
    assert s2.schedulers == []

    s1.schedulers.append("sched")
    assert s1.schedulers == ["sched"]
    assert s2.schedulers == []
