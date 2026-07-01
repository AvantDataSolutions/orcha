"""
Fixtures for the database-utility suite (``orcha.utils.sqlalchemy``).

These tests exercise the backend-specific helpers (``mssql_upsert``,
``postgres_upsert``, ``sqlite_upsert``, ``sqlalchemy_replace``, ``get``,
``get_latest_versions``) against real database engines.

Each backend gates itself:

- **sqlite** always runs (an on-disk temp database, no external service).
- **mssql** runs when ``ORCHA_MSSQL_USER/PASSWORD/SERVER/DB`` are set, and
  skips otherwise. Bring the engine up with
  ``docker compose -f orcha/tests/docker-compose.yml up -d orcha-tests-mssql``.
- **postgres** runs when the core ``ORCHA_CORE_*`` vars are set (it reuses the
  shared test Postgres), and skips otherwise.

This suite deliberately does NOT require ``orcha`` to be initialised: the
helpers under test operate on arbitrary tables, so no orcha schema is needed.
Run it explicitly (it is not part of the default ``testpaths``):

    pytest orcha/tests/db
"""
from __future__ import annotations

import os
from typing import Callable

import pandas as pd
import pytest
from sqlalchemy import Column, MetaData, Table, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from orcha.utils.sqlalchemy import (
    mssql_partial_scaffold,
    sqlite_partial_scaffold,
)

# --- Environment gating ------------------------------------------------------

_MSSQL = {
    "user": os.getenv("ORCHA_MSSQL_USER", ""),
    "passwd": os.getenv("ORCHA_MSSQL_PASSWORD", ""),
    "server": os.getenv("ORCHA_MSSQL_SERVER", ""),
    "db": os.getenv("ORCHA_MSSQL_DB", ""),
}
MISSING_MSSQL_ENV = not all(_MSSQL.values())

_PG = {
    "user": os.getenv("ORCHA_CORE_USER", ""),
    "passwd": os.getenv("ORCHA_CORE_PASSWORD", ""),
    "server": os.getenv("ORCHA_CORE_SERVER", ""),
    "db": os.getenv("ORCHA_CORE_DB", ""),
}
MISSING_PG_ENV = not all(_PG.values())


# --- Backend harness ---------------------------------------------------------

_DEFAULT = object()


class Backend:
    """
    A thin wrapper bundling an engine + sessionmaker with helpers to create
    throwaway tables and read them back, so tests stay declarative. Tables
    created via ``make_table`` are dropped automatically at fixture teardown.
    """

    def __init__(self, engine: Engine, s_maker: sessionmaker[Session], default_schema: str | None):
        self.engine = engine
        self.s_maker = s_maker
        self.default_schema = default_schema
        self._created: list[Table] = []

    def make_table(self, name: str, columns: list[Column], schema=_DEFAULT) -> Table:
        schema = self.default_schema if schema is _DEFAULT else schema
        table = Table(name, MetaData(schema=schema), *columns)
        table.drop(self.engine, checkfirst=True)
        table.create(self.engine)
        self._created.append(table)
        return table

    def rows(self, table: Table, order_by: list | None = None) -> list[tuple]:
        """Return every row of ``table`` as a list of plain tuples."""
        with self.s_maker.begin() as s:
            stmt = table.select()
            if order_by is not None:
                stmt = stmt.order_by(*order_by)
            return [tuple(r) for r in s.execute(stmt).all()]

    def cleanup(self) -> None:
        for table in reversed(self._created):
            try:
                table.drop(self.engine, checkfirst=True)
            except Exception:
                pass
        self._created.clear()


# --- sqlite (always available) ----------------------------------------------

@pytest.fixture
def sqlite(tmp_path) -> Backend:
    """An on-disk sqlite backend (a fresh file per test)."""
    engine, s_maker = sqlite_partial_scaffold(str(tmp_path / "db_suite.sqlite"))
    backend = Backend(engine, s_maker, default_schema=None)
    yield backend
    backend.cleanup()
    engine.dispose()


# --- mssql (gated on ORCHA_MSSQL_*) -----------------------------------------

@pytest.fixture(scope="session")
def _mssql_ready() -> None:
    """
    Ensure the target MSSQL database exists, creating it from ``master`` if
    needed. CREATE DATABASE cannot run inside a transaction, so use an
    AUTOCOMMIT connection.
    """
    if MISSING_MSSQL_ENV:
        pytest.skip(
            "Set ORCHA_MSSQL_USER/PASSWORD/SERVER/DB to run the MSSQL tests "
            "(docker compose up -d orcha-tests-mssql)"
        )
    master_engine, _ = mssql_partial_scaffold(
        _MSSQL["user"], _MSSQL["passwd"], _MSSQL["server"], "master"
    )
    with master_engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(f"IF DB_ID('{_MSSQL['db']}') IS NULL CREATE DATABASE [{_MSSQL['db']}];")
        )
    master_engine.dispose()


@pytest.fixture
def mssql(_mssql_ready) -> Backend:
    """A SQL Server backend pointed at the ORCHA_MSSQL_* database."""
    engine, s_maker = mssql_partial_scaffold(
        _MSSQL["user"], _MSSQL["passwd"], _MSSQL["server"], _MSSQL["db"]
    )
    backend = Backend(engine, s_maker, default_schema="dbo")
    yield backend
    backend.cleanup()
    engine.dispose()


def _mssql_master_exec(*statements: str) -> None:
    """Run AUTOCOMMIT statements against master (CREATE/DROP DATABASE can't be
    transactional)."""
    engine, _ = mssql_partial_scaffold(
        _MSSQL["user"], _MSSQL["passwd"], _MSSQL["server"], "master"
    )
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        for stmt in statements:
            conn.execute(text(stmt))
    engine.dispose()


@pytest.fixture
def mssql_odd_collation(_mssql_ready) -> Backend:
    """
    A SQL Server database whose collation deliberately DIFFERS from the server
    (tempdb) collation.

    This reproduces the real-world scenario ``mssql_upsert``'s collation logic
    exists for: a freshly-created ``#temp`` table inherits the *server*
    collation, so MERGE-joining it on a string key against a table in this
    differently-collated database raises a collation conflict (error 468)
    unless the temp table's collation is realigned first.

    The database is created fresh and dropped at teardown.
    """
    server_engine, _ = mssql_partial_scaffold(
        _MSSQL["user"], _MSSQL["passwd"], _MSSQL["server"], "master"
    )
    with server_engine.connect() as conn:
        server_coll = conn.execute(
            text("SELECT CAST(SERVERPROPERTY('Collation') AS varchar(200))")
        ).scalar()
    server_engine.dispose()

    # Pick any collation that differs from the server's, so the test genuinely
    # exercises the mismatch path regardless of the host's server collation.
    odd_coll = (
        "Latin1_General_CS_AS"
        if server_coll != "Latin1_General_CS_AS"
        else "SQL_Latin1_General_CP1_CI_AS"
    )
    db_name = "orcha_test_oddcoll"

    drop_sql = (
        f"IF DB_ID('{db_name}') IS NOT NULL BEGIN "
        f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
        f"DROP DATABASE [{db_name}]; END"
    )
    _mssql_master_exec(drop_sql, f"CREATE DATABASE [{db_name}] COLLATE {odd_coll}")

    engine, s_maker = mssql_partial_scaffold(
        _MSSQL["user"], _MSSQL["passwd"], _MSSQL["server"], db_name
    )
    backend = Backend(engine, s_maker, default_schema="dbo")
    yield backend
    backend.cleanup()
    engine.dispose()
    _mssql_master_exec(drop_sql)


# --- postgres (gated on ORCHA_CORE_*) ---------------------------------------

@pytest.fixture(scope="session")
def _pg_engine():
    if MISSING_PG_ENV:
        pytest.skip("Set ORCHA_CORE_USER/PASSWORD/SERVER/DB to run the Postgres tests")
    from sqlalchemy import create_engine

    engine = create_engine(
        f"postgresql+psycopg://{_PG['user']}:{_PG['passwd']}@{_PG['server']}/{_PG['db']}"
    )
    yield engine
    engine.dispose()


@pytest.fixture
def postgres(_pg_engine) -> Backend:
    """A Postgres backend reusing the shared core test database (schema 'public')."""
    s_maker = sessionmaker(bind=_pg_engine)
    backend = Backend(_pg_engine, s_maker, default_schema="public")
    yield backend
    backend.cleanup()


# --- shared row helper -------------------------------------------------------

@pytest.fixture
def df() -> Callable[..., pd.DataFrame]:
    """Convenience factory so tests read as ``df(id=[1,2], name=['a','b'])``."""
    def _make(**columns) -> pd.DataFrame:
        return pd.DataFrame(columns)
    return _make
