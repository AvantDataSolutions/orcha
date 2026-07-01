"""
Verify the API guards against use before ``initialise()``.

These tests are order-dependent by nature: every access must raise until
``test_z_initialise_succeeds`` runs, so it (and the post-init check) are named to
sort last. Keep this suite in its own process — once orcha is initialised it
cannot be un-initialised.
"""
from __future__ import annotations

from datetime import datetime as dt

import pytest

SINCE = dt(2020, 1, 1)

from orcha.core import initialise, tasks
from orcha.core.tasks import RunItem, ScheduleSet, TaskItem

from helpers import ORCHA_DB, ORCHA_PASSWORD, ORCHA_SERVER, ORCHA_USER, noop_task


def _create_task():
    return TaskItem.create(
        task_idk="uninit_task",
        name="uninit",
        description="uninit",
        schedule_sets=[ScheduleSet("* * * * *", {})],
        task_function=noop_task,
    )


def test_a_confirm_initialised_raises():
    with pytest.raises(RuntimeError):
        tasks.confirm_initialised()


def test_a_task_get_all_raises():
    with pytest.raises(RuntimeError):
        TaskItem.get_all()


def test_a_task_get_one_raises():
    with pytest.raises(RuntimeError):
        TaskItem.get("not_a_task")


def test_a_task_create_raises():
    with pytest.raises(RuntimeError):
        _create_task()


def test_a_run_get_all_raises():
    with pytest.raises(RuntimeError):
        RunItem.get_all(task="", since=SINCE)


def test_a_run_get_one_raises():
    with pytest.raises(RuntimeError):
        RunItem.get("not_a_run")


def test_a_run_get_all_queued_raises():
    with pytest.raises(RuntimeError):
        RunItem.get_all_queued(task="")


def test_a_run_get_running_raises():
    with pytest.raises(RuntimeError):
        RunItem.get_running_runs(task="")


def test_z_initialise_succeeds():
    initialise(
        orcha_user=ORCHA_USER,
        orcha_pass=ORCHA_PASSWORD,
        orcha_server=ORCHA_SERVER,
        orcha_db=ORCHA_DB,
        application_name="orcha_tests_uninitialised",
    )
    tasks.confirm_initialised()


def test_z_task_runner_required_after_init():
    # Now initialised, but no TaskRunner registered: creating a task that asks to
    # register with the runner must raise.
    with pytest.raises(Exception):
        _create_task()
