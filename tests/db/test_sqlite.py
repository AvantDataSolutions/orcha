"""
Tests for ``orcha.utils.sqlalchemy.sqlite_upsert`` and ``sqlalchemy_replace``
against an on-disk sqlite database (always available, no external service).

Argument order here is ``(session, table, data, ...)`` — note this differs from
``mssql_upsert``, which takes data first.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import Column, Integer, String

from orcha.utils.sqlalchemy import sqlalchemy_replace, sqlite_upsert


def _cols():
    return [
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(50)),
        Column("val", Integer),
    ]


def test_inserts_new_rows(sqlite, df):
    table = sqlite.make_table("upsert_insert", _cols())

    sqlite_upsert(sqlite.s_maker, table, df(id=[1, 2, 3], name=["a", "b", "c"], val=[10, 20, 30]))

    assert sqlite.rows(table, order_by=[table.c.id]) == [(1, "a", 10), (2, "b", 20), (3, "c", 30)]


def test_updates_and_inserts(sqlite, df):
    table = sqlite.make_table("upsert_mixed", _cols())
    sqlite_upsert(sqlite.s_maker, table, df(id=[1, 2, 3], name=["a", "b", "c"], val=[10, 20, 30]))

    sqlite_upsert(sqlite.s_maker, table, df(id=[2, 4], name=["B", "d"], val=[200, 40]))

    assert sqlite.rows(table, order_by=[table.c.id]) == [
        (1, "a", 10),
        (2, "B", 200),
        (3, "c", 30),
        (4, "d", 40),
    ]


def test_empty_dataframe_is_noop(sqlite):
    table = sqlite.make_table("upsert_empty", _cols())

    result = sqlite_upsert(sqlite.s_maker, table, pd.DataFrame(columns=["id", "name", "val"]))

    assert result is None
    assert sqlite.rows(table) == []


def test_chunking_covers_all_rows(sqlite, df):
    """More rows than the chunk size must still all be written."""
    table = sqlite.make_table("upsert_chunked", _cols())
    n = 2500
    sqlite_upsert(
        sqlite.s_maker,
        table,
        df(id=list(range(n)), name=["x"] * n, val=list(range(n))),
        chunksize=500,
    )

    assert len(sqlite.rows(table)) == n


def test_replace_swaps_all_data(sqlite, df):
    table = sqlite.make_table("replace_target", _cols())
    sqlite_upsert(sqlite.s_maker, table, df(id=[1, 2, 3], name=["a", "b", "c"], val=[10, 20, 30]))

    # replace drops everything first, then inserts the new frame.
    sqlalchemy_replace(sqlite.s_maker, table, df(id=[9], name=["z"], val=[99]))

    assert sqlite.rows(table, order_by=[table.c.id]) == [(9, "z", 99)]


def test_replace_converts_nan_to_null(sqlite):
    """
    sqlalchemy_replace converts NaN to None (the pymssql NaN workaround); the
    stored value must be a real NULL, not the string 'nan' or a NaN float.
    """
    table = sqlite.make_table("replace_nan", _cols())
    data = pd.DataFrame({"id": [1], "name": ["a"], "val": [np.nan]})

    sqlalchemy_replace(sqlite.s_maker, table, data)

    assert sqlite.rows(table) == [(1, "a", None)]
