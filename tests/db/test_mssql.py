"""
Tests for the SQL Server helpers in ``orcha.utils.sqlalchemy`` (currently just
``mssql_upsert``), run against a real SQL Server.

Gated on the ORCHA_MSSQL_* env vars via the ``mssql`` fixture (see conftest).

Note the argument order: ``mssql_upsert(data, s_maker, table, ...)`` — data
comes first, unlike the postgres/sqlite variants.

Primary-key columns use ``autoincrement=False``: ``mssql_upsert`` supplies
explicit PK values, which SQL Server rejects on an IDENTITY column
(``IDENTITY_INSERT is OFF``).
"""
from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, String, text
from sqlalchemy.dialects.mssql import NVARCHAR

from orcha.utils.sqlalchemy import mssql_upsert


def _pk(name="id"):
    return Column(name, Integer, primary_key=True, autoincrement=False)


def test_inserts_new_rows(mssql, df):
    table = mssql.make_table("upsert_insert", [_pk(), Column("name", String(50)), Column("val", Integer)])

    mssql_upsert(df(id=[1, 2, 3], name=["a", "b", "c"], val=[10, 20, 30]), mssql.s_maker, table)

    assert mssql.rows(table, order_by=[table.c.id]) == [(1, "a", 10), (2, "b", 20), (3, "c", 30)]


def test_updates_existing_rows(mssql, df):
    table = mssql.make_table("upsert_update", [_pk(), Column("name", String(50)), Column("val", Integer)])
    mssql_upsert(df(id=[1, 2], name=["a", "b"], val=[10, 20]), mssql.s_maker, table)

    # Same PKs, changed non-PK columns -> update in place, no new rows.
    mssql_upsert(df(id=[1, 2], name=["A", "B"], val=[11, 22]), mssql.s_maker, table)

    assert mssql.rows(table, order_by=[table.c.id]) == [(1, "A", 11), (2, "B", 22)]


def test_mixed_insert_and_update(mssql, df):
    table = mssql.make_table("upsert_mixed", [_pk(), Column("name", String(50)), Column("val", Integer)])
    mssql_upsert(df(id=[1, 2, 3], name=["a", "b", "c"], val=[10, 20, 30]), mssql.s_maker, table)

    # Update 2, leave 1 & 3 untouched, insert 4.
    mssql_upsert(df(id=[2, 4], name=["B", "d"], val=[200, 40]), mssql.s_maker, table)

    assert mssql.rows(table, order_by=[table.c.id]) == [
        (1, "a", 10),
        (2, "B", 200),
        (3, "c", 30),
        (4, "d", 40),
    ]


def test_empty_dataframe_is_noop(mssql):
    table = mssql.make_table("upsert_empty", [_pk(), Column("name", String(50))])

    result = mssql_upsert(pd.DataFrame(columns=["id", "name"]), mssql.s_maker, table)

    assert result is None
    assert mssql.rows(table) == []


def test_composite_primary_key(mssql, df):
    table = mssql.make_table(
        "upsert_composite",
        [_pk("a"), _pk("b"), Column("val", Integer)],
    )
    mssql_upsert(df(a=[1, 1], b=[1, 2], val=[10, 20]), mssql.s_maker, table)
    # Update only (a=1, b=2); (a=1, b=1) is unchanged.
    mssql_upsert(df(a=[1], b=[2], val=[99]), mssql.s_maker, table)

    assert mssql.rows(table, order_by=[table.c.a, table.c.b]) == [(1, 1, 10), (1, 2, 99)]


def test_wide_table_many_columns(mssql):
    """
    A wide table upserted as a single large chunk: 12 columns x 300 rows.

    (SQL Server has a 2100-parameter-per-statement limit, but pymssql
    substitutes parameters client-side into the SQL text rather than binding
    them server-side, so ``method='multi'`` does not hit that limit here. This
    guards against regressions if the driver or insert method ever changes.)
    """
    n_cols = 12
    n_rows = 300
    value_cols = [f"c{i}" for i in range(n_cols - 1)]
    columns = [_pk()] + [Column(c, Integer) for c in value_cols]
    table = mssql.make_table("upsert_wide", columns)

    data = {"id": list(range(n_rows))}
    for c in value_cols:
        data[c] = list(range(n_rows))
    mssql_upsert(pd.DataFrame(data), mssql.s_maker, table)

    assert len(mssql.rows(table)) == n_rows


def test_all_columns_are_primary_key(mssql, df):
    """
    A table whose columns are all part of the primary key has no non-PK columns
    to update, so the MERGE has nothing to SET on a match. The upsert should
    still succeed (inserting new rows, no-op on existing) by omitting the
    WHEN MATCHED clause entirely.
    """
    table = mssql.make_table("upsert_all_pk", [_pk("a"), _pk("b")])

    mssql_upsert(df(a=[1, 2], b=[10, 20]), mssql.s_maker, table)
    # Re-upsert an existing key (matches -> nothing to update) plus a new key.
    mssql_upsert(df(a=[1, 3], b=[10, 30]), mssql.s_maker, table)

    assert mssql.rows(table, order_by=[table.c.a, table.c.b]) == [(1, 10), (2, 20), (3, 30)]


# --- collation: database collation differs from server/tempdb collation ------
#
# The real reason mssql_upsert has collation logic: pushing into a database
# whose collation differs from the server collation. A freshly-created #temp
# table (which mssql_upsert builds via to_sql) inherits the *server* collation,
# so MERGE-joining it on a string key against a table in the differently-collated
# database fails with a collation conflict (error 468) unless the temp table's
# collation is first realigned to the database collation. These use the
# ``mssql_odd_collation`` fixture, whose DB is created with a non-server collation.

def _str_key_cols():
    return [Column("k", NVARCHAR(50), primary_key=True), Column("v", NVARCHAR(50))]


def test_odd_collation_fixture_really_mismatches(mssql_odd_collation):
    """
    Guards that the scenario is genuinely being exercised: the fixture's database
    collation must differ from the server collation, otherwise the collation
    tests below would pass trivially and prove nothing.
    """
    with mssql_odd_collation.s_maker.begin() as s:
        server_coll = s.execute(
            text("SELECT CAST(SERVERPROPERTY('Collation') AS varchar(200))")
        ).scalar()
        db_coll = s.execute(
            text("SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS varchar(200))")
        ).scalar()

    assert server_coll != db_coll


def test_raw_merge_conflicts_without_collation_fix(mssql_odd_collation):
    """
    Demonstrates the failure mode mssql_upsert defends against: a fresh #temp
    table (server collation) cannot be MERGE-joined on a string key against a
    table in the odd-collation database. This is the exact error users hit
    before the collation-realignment logic was added.
    """
    mssql_odd_collation.make_table("coll_raw", _str_key_cols())

    with pytest.raises(Exception) as exc_info:
        with mssql_odd_collation.s_maker.begin() as s:
            # Fresh temp table -> inherits the server/tempdb collation.
            s.execute(text("CREATE TABLE #raw (k NVARCHAR(50), v NVARCHAR(50))"))
            s.execute(text("INSERT INTO #raw (k, v) VALUES (N'a', N'1')"))
            s.execute(text(
                "MERGE dbo.coll_raw AS tgt USING #raw AS src ON (src.k = tgt.k) "
                "WHEN NOT MATCHED THEN INSERT (k, v) VALUES (src.k, src.v);"
            ))

    assert "collation conflict" in str(exc_info.value).lower()


def test_upsert_succeeds_across_collation_mismatch(mssql_odd_collation, df):
    """
    mssql_upsert must transparently handle the db/server collation mismatch: with
    a string primary key (so the MERGE ON does a string comparison), the upsert
    should insert and update correctly rather than raising a collation conflict.
    """
    table = mssql_odd_collation.make_table("coll_upsert", _str_key_cols())

    mssql_upsert(df(k=["a", "b"], v=["1", "2"]), mssql_odd_collation.s_maker, table)
    # Update b, insert c -> exercises both MERGE branches on the string key.
    mssql_upsert(df(k=["b", "c"], v=["22", "3"]), mssql_odd_collation.s_maker, table)

    assert sorted(mssql_odd_collation.rows(table)) == [("a", "1"), ("b", "22"), ("c", "3")]
