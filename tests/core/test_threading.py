"""
Timeout helper isolation and the run fence (RES-1 / RES-2).

RES-2: exception/timeout state must be keyed by a per-call token, not the thread
name, so two concurrent runs (which may share a thread name) can't read each
other's stored exception/timeout state.

RES-1: a task that exceeds its timeout raises a distinct TaskTimeoutException,
and once a run is fenced any further writes it makes are dropped (the abandoned
worker thread cannot be stopped, so the fence stops it from mutating a run that
has already been failed).
"""
from __future__ import annotations

import threading as pythread
import time

import pytest

from orcha.utils import threading as orcha_threading
from orcha.core.tasks import RunItem


# --- RES-2: per-token isolation ----------------------------------------------

def test_stored_exceptions_are_isolated_by_token():
    orcha_threading.store_exception(ValueError("a"), token="run-a")
    orcha_threading.store_exception(RuntimeError("b"), token="run-b")

    # Each token reads back only its own exception (and get clears it).
    assert isinstance(orcha_threading.get_exception("run-a"), ValueError)
    assert isinstance(orcha_threading.get_exception("run-b"), RuntimeError)
    assert orcha_threading.get_exception("run-a") is None


def test_concurrent_runs_sharing_a_thread_name_do_not_cross_contaminate():
    # Two functions run concurrently under the SAME thread name (as the real
    # runner does with its progress helper) but with distinct tokens. Each
    # caller must receive its own exception, not the other run's.
    barrier = pythread.Barrier(2)
    results: dict[str, str] = {}

    def make_and_run(token: str, value: str):
        def f():
            # Ensure both workers are alive at the same time before raising, so a
            # thread-name-keyed store would genuinely collide.
            barrier.wait(timeout=5)
            raise ValueError(value)

        try:
            orcha_threading.run_function_with_timeout(
                5, "unused", f, token=token, thread_name="shared-name",
            )
        except ValueError as e:
            results[token] = str(e)

    threads = [
        pythread.Thread(target=make_and_run, args=("A", "A")),
        pythread.Thread(target=make_and_run, args=("B", "B")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert results == {"A": "A", "B": "B"}


# --- RES-1: timeout signalling + cleanup -------------------------------------

def test_timeout_raises_task_timeout_exception():
    def slow():
        time.sleep(5)

    with pytest.raises(orcha_threading.TaskTimeoutException):
        orcha_threading.run_function_with_timeout(1, "timed out", slow, token="slow-run")


def test_timeout_remainder_is_cleaned_up():
    def quick():
        return None

    orcha_threading.run_function_with_timeout(5, "unused", quick, token="quick-run")
    # Per-token keying would leak an entry per run without cleanup.
    assert "quick-run" not in orcha_threading._timeout_remainders


def test_normal_exception_is_reraised_not_wrapped_as_timeout():
    def boom():
        raise KeyError("nope")

    with pytest.raises(KeyError):
        orcha_threading.run_function_with_timeout(5, "unused", boom, token="boom-run")


# --- RES-1: the run fence (DB) -----------------------------------------------

def test_fenced_run_drops_writes(make_task, clock):
    task = make_task(idk="fence_task")
    run = task.schedule_run(schedule_by_id="test", schedule=task.schedule_sets[0])
    assert run is not None

    # The run is failed (as the timeout handler would do) then fenced.
    run.set_status("failed", output={"message": "timed out"})
    run.fence()

    # A late write from this (abandoned) run object is dropped.
    run.set_output({"late_write": "should not persist"}, merge=True)

    reloaded = RunItem.get(run_id=run.run_idk, task=task)
    assert reloaded is not None
    assert reloaded.status == "failed"
    assert "late_write" not in (reloaded.output or {})


def test_fence_survives_reload(make_task, clock):
    task = make_task(idk="fence_reload_task")
    run = task.schedule_run(schedule_by_id="test", schedule=task.schedule_sets[0])
    assert run is not None
    run.fence()
    run.reload()  # reload must not clear the fence
    assert run._is_fenced is True
