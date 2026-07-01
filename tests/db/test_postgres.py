"""
Tests for the Postgres-backed helpers in ``orcha.utils.sqlalchemy``:
``postgres_upsert`` plus the read helpers ``get`` and ``get_latest_versions``
(``get_latest_versions`` uses Postgres-only ``DISTINCT ON`` syntax).

Self-skips unless the ORCHA_CORE_* env vars point at a Postgres instance (see
the ``postgres`` fixture in conftest). ``postgres_upsert``'s argument order is
``(session, table, data, ...)``.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import Column, Integer, String

from orcha.utils.sqlalchemy import get, get_latest_versions, postgres_upsert


def _cols():
    return [
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(50)),
        Column("val", Integer),
    ]


def _load(backend, name, columns, data_df):
    table = backend.make_table(name, columns)
    postgres_upsert(backend.s_maker, table, data_df)
    return table


# --- postgres_upsert ---------------------------------------------------------

def test_inserts_new_rows(postgres, df):
    table = postgres.make_table("pg_upsert_insert", _cols())

    postgres_upsert(postgres.s_maker, table, df(id=[1, 2, 3], name=["a", "b", "c"], val=[10, 20, 30]))

    assert postgres.rows(table, order_by=[table.c.id]) == [(1, "a", 10), (2, "b", 20), (3, "c", 30)]


def test_on_conflict_updates_existing(postgres, df):
    table = postgres.make_table("pg_upsert_conflict", _cols())
    postgres_upsert(postgres.s_maker, table, df(id=[1, 2, 3], name=["a", "b", "c"], val=[10, 20, 30]))

    postgres_upsert(postgres.s_maker, table, df(id=[2, 4], name=["B", "d"], val=[200, 40]))

    assert postgres.rows(table, order_by=[table.c.id]) == [
        (1, "a", 10),
        (2, "B", 200),
        (3, "c", 30),
        (4, "d", 40),
    ]


def test_empty_dataframe_is_noop(postgres):
    table = postgres.make_table("pg_upsert_empty", _cols())

    result = postgres_upsert(postgres.s_maker, table, pd.DataFrame(columns=["id", "name", "val"]))

    assert result is None
    assert postgres.rows(table) == []


def test_chunking_covers_all_rows(postgres, df):
    table = postgres.make_table("pg_upsert_chunked", _cols())
    n = 2500
    postgres_upsert(
        postgres.s_maker,
        table,
        df(id=list(range(n)), name=["x"] * n, val=list(range(n))),
        chunksize=500,
    )

    assert len(postgres.rows(table)) == n


# --- get ---------------------------------------------------------------------

def test_get_all_rows(postgres, df):
    cols = [Column("id", Integer, primary_key=True, autoincrement=False), Column("name", String(50))]
    _load(postgres, "get_all", cols, df(id=[1, 2, 3], name=["a", "b", "c"]))

    rows = get(postgres.s_maker, "public.get_all", ["id", "name"])

    assert sorted(tuple(r) for r in rows) == [(1, "a"), (2, "b"), (3, "c")]


def test_get_star_selects_all_columns(postgres, df):
    cols = [Column("id", Integer, primary_key=True, autoincrement=False), Column("name", String(50))]
    _load(postgres, "get_star", cols, df(id=[1], name=["a"]))

    rows = get(postgres.s_maker, "public.get_star", "*")

    assert [tuple(r) for r in rows] == [(1, "a")]


def test_get_with_match_pair(postgres, df):
    cols = [Column("id", Integer, primary_key=True, autoincrement=False), Column("name", String(50))]
    _load(postgres, "get_match", cols, df(id=[1, 2, 3], name=["a", "b", "c"]))

    rows = get(postgres.s_maker, "public.get_match", ["name"], match_pairs=[("id", "=", 2)])

    assert [tuple(r) for r in rows] == [("b",)]


def test_get_or_match_type(postgres, df):
    cols = [Column("id", Integer, primary_key=True, autoincrement=False), Column("name", String(50))]
    _load(postgres, "get_or", cols, df(id=[1, 2, 3], name=["a", "b", "c"]))

    rows = get(
        postgres.s_maker,
        "public.get_or",
        ["id"],
        match_pairs=[("id", "=", 1), ("id", "=", 3)],
        match_type="OR",
    )

    assert sorted(tuple(r) for r in rows) == [(1,), (3,)]


# --- get_latest_versions -----------------------------------------------------

def test_get_latest_versions_returns_highest_version_per_key(postgres, df):
    cols = [
        Column("key", Integer, primary_key=True, autoincrement=False),
        Column("version", Integer, primary_key=True, autoincrement=False),
        Column("payload", String(50)),
    ]
    _load(
        postgres,
        "versions",
        cols,
        df(
            key=[1, 1, 2, 2],
            version=[1, 2, 1, 3],
            payload=["k1v1", "k1v2", "k2v1", "k2v3"],
        ),
    )

    rows = get_latest_versions(
        postgres.s_maker,
        "public.versions",
        key_columns=["key"],
        version_column="version",
        select_columns=["key", "version", "payload"],
    )

    assert sorted(tuple(r) for r in rows) == [(1, 2, "k1v2"), (2, 3, "k2v3")]
