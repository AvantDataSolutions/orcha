"""
Shared pytest configuration for the Orcha test suites.

Loaded for every suite under ``orcha/tests`` (both ``core`` and
``uninitialised``). It only provides things that are safe regardless of whether
``orcha`` has been initialised:

- Makes the repo root (for ``import orcha``) and this directory (for
  ``import helpers``) importable no matter where pytest is invoked from.
- A controllable clock built on the ``orcha.set_time`` / ``orcha.reset_time``
  seam, plus an autouse fixture that resets it after every test so a frozen
  clock never leaks.
- Skips the DB-backed tests with a clear reason if the DB env vars are unset.

Suite-specific setup (initialising orcha, per-test DB cleaning, the scheduler
fixture, etc.) lives in the per-suite conftest files. Reusable, non-fixture
helpers live in ``helpers.py``.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime as dt

import pytest

# Put the repo root (so `import orcha` works) and this tests directory (so
# `import helpers` works) on sys.path, independent of pytest's import mode.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "..", ".."))
for _p in (_REPO_ROOT, _THIS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import orcha  # noqa: E402  (import after sys.path tweak, by design)
from helpers import MISSING_DB_ENV, FakeClock  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_clock():
    """Always clear any clock override once a test finishes."""
    yield
    orcha.reset_time()


@pytest.fixture
def clock() -> FakeClock:
    """
    Install a FakeClock as orcha's time source, frozen at a fixed instant on a
    cron boundary (2026-01-01 00:00:00). Tests advance it explicitly.
    """
    fake = FakeClock(dt(2026, 1, 1, 0, 0, 0))
    orcha.set_time(fake)
    return fake


def pytest_collection_modifyitems(config, items):
    """
    Skip the Postgres-backed suites with a clear reason if the core DB env vars
    are not set. Only the ``core`` and ``uninitialised`` suites need the shared
    Postgres instance; the ``db`` suite gates itself per-backend (its
    fixtures skip individually when their own env vars are unset), so it must
    not be caught by this blanket skip.
    """
    if not MISSING_DB_ENV:
        return
    skip = pytest.mark.skip(
        reason="Set ORCHA_CORE_USER/PASSWORD/SERVER/DB to run the DB-backed tests"
    )
    needs_core_db = (f"{os.sep}core{os.sep}", f"{os.sep}uninitialised{os.sep}")
    for item in items:
        if any(part in str(item.fspath) for part in needs_core_db):
            item.add_marker(skip)
