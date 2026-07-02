"""
Shared, framework-agnostic test helpers.

Kept as a plain importable module (not a conftest) so both the root and
per-suite conftests, and the test modules, can ``import helpers`` without
relying on cross-conftest relative imports. The tests directory is placed on
``sys.path`` by the root conftest, so ``import helpers`` resolves here.
"""
from __future__ import annotations

import os
from datetime import datetime as dt
from datetime import timedelta as td

import sqlalchemy
from sqlalchemy import text

# --- Database connection (from the environment) ------------------------------

ORCHA_USER = os.getenv("ORCHA_CORE_USER", "")
ORCHA_PASSWORD = os.getenv("ORCHA_CORE_PASSWORD", "")
ORCHA_SERVER = os.getenv("ORCHA_CORE_SERVER", "")
ORCHA_DB = os.getenv("ORCHA_CORE_DB", "")

MISSING_DB_ENV = not all([ORCHA_USER, ORCHA_PASSWORD, ORCHA_SERVER, ORCHA_DB])

# Tables truncated between tests to give each test a clean slate. Truncate
# (rather than transactional rollback) is used deliberately: the scheduler and
# task runner open their own DB connections/sessions, so a rollback on the
# test's connection would neither see nor undo their writes. Guarded with
# IF EXISTS so the helper is safe before migrations have run.
_TRUNCATE_TABLES = [
    ("orcha", "tasks"),
    ("orcha", "runs"),
    ("orcha", "schedulers"),
    ("orcha_logs", "logs"),
    ("message_queue", "messages"),
]

_engine: sqlalchemy.Engine | None = None


def get_engine() -> sqlalchemy.Engine:
    """A lazily-created engine used only by ``empty_database``."""
    global _engine
    if _engine is None:
        # Match the driver orcha itself uses (psycopg v3), not the psycopg2 default.
        _engine = sqlalchemy.create_engine(
            f"postgresql+psycopg://{ORCHA_USER}:{ORCHA_PASSWORD}@{ORCHA_SERVER}/{ORCHA_DB}"
        )
    return _engine


def empty_database() -> None:
    """Truncate all orcha tables so a test starts from a known-empty state."""
    checks = "\n".join(
        f"""
        IF EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table}'
        ) THEN
            EXECUTE 'TRUNCATE TABLE {schema}.{table}';
        END IF;
        """
        for schema, table in _TRUNCATE_TABLES
    )
    with get_engine().begin() as conn:
        conn.execute(text(f"DO $$\nBEGIN\n{checks}\nEND $$;"))


# --- Controllable clock ------------------------------------------------------

def noop_task(task_item, run_item, cfg):
    """Default no-op task function for tests (defined at module level, as apps do)."""
    return None


class FakeClock:
    """
    A deterministic, advanceable clock for ``orcha.set_time``.

    Callable so it can be handed straight to ``orcha.set_time``; ``set`` jumps to
    an absolute time and ``advance`` moves forward by a delta. This lets tests
    drive the scheduler across cron boundaries without waiting on wall-clock time.
    """

    def __init__(self, start: dt):
        self._now = start

    def __call__(self) -> dt:
        return self._now

    def set(self, when: dt) -> None:
        self._now = when

    def advance(self, delta: td) -> None:
        self._now += delta
