from __future__ import annotations

import re
from typing import Literal, Type

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine.mock import MockConnection
from sqlalchemy.engine.row import Row
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql import text as sql

from orcha.core.credentials import (ORCHA_CORE_DB, ORCHA_CORE_PASSWORD,
                                    ORCHA_CORE_SERVER, ORCHA_CORE_USER,
                                    check_credentials)

DeclarativeBaseType = Type[declarative_base()]

SCAFFOLD_CACHE: dict[str, tuple[DeclarativeBaseType, MockConnection, sessionmaker]] = {}

def postgres_scaffold(schema_name: str):
    check_credentials()
    if schema_name not in SCAFFOLD_CACHE:
        engine = create_engine(
            f'postgresql://{ORCHA_CORE_USER}:{ORCHA_CORE_PASSWORD}@{ORCHA_CORE_SERVER}/{ORCHA_CORE_DB}',
            pool_size=50,
            max_overflow=2,
            pool_recycle=300,
            pool_use_lifo=True
        )
        Base = declarative_base(metadata=MetaData(schema=schema_name, bind=engine))
        session = sessionmaker(bind=engine)

        SCAFFOLD_CACHE[schema_name] = (Base, engine, session)
    if schema_name in SCAFFOLD_CACHE:
        return SCAFFOLD_CACHE[schema_name]
    else:
        raise Exception('Failed to create scaffold for: ' + schema_name)


def postgres_build(base: DeclarativeBaseType, engine: MockConnection, schema_name: str):
    engine_inspect = inspect(engine)
    if engine_inspect is None:
        raise Exception('Engine inspect failed for schema: ' + schema_name)
    if schema_name not in engine_inspect.get_schema_names():
        engine.execute(CreateSchema(schema_name))
    base.metadata.create_all(engine, checkfirst=True) # type: ignore


def postgres_upsert(
        base: DeclarativeBaseType, session: sessionmaker,
        table: str, data: pd.DataFrame
    ) -> None:
    if len(data) == 0:
        return None

    if '.' not in table:
        raise Exception('Table must be in the format schema.table')

    _, table = table.split('.')

    with session.begin() as db:
        connection = db.get_bind()
        table_obj = Table(table, base.metadata, autoload=True, autoload_with=connection) # type: ignore
        for chunk in [data[i:i+1000] for i in range(0, len(data), 1000)]:
            stmt = insert(table_obj).values(chunk.to_dict('records'))
            table_inspect = inspect(table_obj)
            if table_inspect is None:
                return None
            update_dict = {}
            for column in table_inspect.columns:
                if column.primary_key:
                    continue
                update_dict[column.name] = getattr(stmt.excluded, column.name)

            stmt = stmt.on_conflict_do_update(
                index_elements=[column.name for column in table_inspect.primary_key],
                set_=update_dict
            )
            db.execute(stmt)


def _sanitize_sql(input_str: str):
    # Remove any non-alphanumeric characters except for spaces
    sanitized_str = re.sub(r'[^\w\s]', '', input_str)
    # Replace any spaces with underscores
    sanitized_str = sanitized_str.replace(' ', '_')
    # Return the sanitized string
    return sanitized_str


def get(
        session, table, select_columns: list | Literal['*'],
        match_pairs: list[tuple[str, str, str]] = [],
        match_type: Literal['AND', 'OR'] = 'AND'
    ) -> list[Row]:
    """
    Returns rows from a database table that match the specified criteria.

    Args:
        session: The SQLAlchemy session to use for the database connection.
        table: The name of the database table to query; 'schema.table' format.
        select_columns: A list of column names to include in the query results.
        match_pairs: A list of tuples representing the column name and value to match.
            Defaults to an empty list, which returns all rows in the table.
        match_type: The type of join to use for the match_pairs. Must be 'AND' or 'OR'.
            Defaults to 'AND'.

    Returns:
        A list of tuples representing the query results.
    """
    match_type = _sanitize_sql(match_type) # type: ignore - literal is a string
    if isinstance(select_columns, list):
        # If it's a list, make the appropriate string,
        # otherwise we just leave it as '*'
        select_columns = ', '.join([_sanitize_sql(c) for c in select_columns])  # type: ignore

    # Just make sure nothing funny was passed through accidentally
    table, schema = table.split('.')
    table = _sanitize_sql(table)
    schema = _sanitize_sql(schema)
    match_pairs = [(col, compr, test) for col, compr, test in match_pairs]

    # add an index column to match_pairs to handle duplicate keys
    # e.g. where id=5 or id=6, without the index only the last 'id' would be used
    mp_indexed = [(i, col, compr, test) for i, (col, compr, test) in enumerate(match_pairs)]
    if len(match_pairs) == 0:
        pairs_query = ''
    else:
        pairs_query = [f'{_sanitize_sql(k)} {compr} :{f"{i}_{k}"}' for i, k , compr, _ in mp_indexed]
        pairs_query = f' {match_type} '.join(pairs_query)

    q_str = f'''
        SELECT {select_columns}
        FROM {table}.{schema}
        {f'WHERE {pairs_query}' if pairs_query else ''}
    '''
    with session.begin() as tx:
        # only keep the first and last values of each key
        mp_sql_params = {f'{i}_{k}': v for i, k, c, v in mp_indexed}
        # print(sql(q_str).bindparams(**mp_sql_params).compile(
        #     dialect=postgresql.dialect(),
        #     compile_kwargs={"literal_binds": True}
        # ))
        return tx.execute(sql(q_str).bindparams(**mp_sql_params)).all()



def get_latest_versions(
        session, table, key_columns: list, version_column: str,
        select_columns: list | Literal['*'], match_pairs: list[tuple[str, str, str]] = [],
        match_type: Literal['AND', 'OR'] = 'AND'
    ) -> list[Row]:
    """
    Returns the latest versions of rows in a database table that match the specified criteria.
    This is used when a table contains multiple versions of the same key.

    Args:
        session: The SQLAlchemy session to use for the database connection.
        table: The name of the database table to query; 'schema.table' format.
        key_columns: A list of column names that make up the primary key of the table.
        version_column: The name of the column that contains the version number.
        select_columns: A list of column names to include in the query results.
        match_pairs: A list of tuples representing the column name and value to match.
            Defaults to an empty list, which returns all rows in the table.
        match_type: The type of join to use for the match_pairs. Must be 'AND' or 'OR'.
            Defaults to 'AND'.

    Returns:
        A list of tuples representing the query results.
    """
    match_type = _sanitize_sql(match_type) # type: ignore - literal is a string
    # add an index column to match_pairs to handle duplicate keys
    # e.g. where id=5 or id=6, without the index only the last 'id' would be used
    mp_indexed = [(i, col, compr, test) for i, (col, compr, test) in enumerate(match_pairs)]
    if len(match_pairs) == 0:
        pairs_query = ''
    else:
        pairs_query = [f'{_sanitize_sql(k)} {compr} :{f"{i}_{k}"}' for i, k , compr, _ in mp_indexed]
        pairs_query = f' {match_type} '.join(pairs_query)

    if isinstance(select_columns, list):
        # If it's a list, make the appropriate string,
        # otherwise we just leave it as '*'
        select_columns = ', '.join([_sanitize_sql(c) for c in select_columns])  # type: ignore

    # Just make sure nothing funny was passed through accidentally
    key_columns = [_sanitize_sql(c) for c in key_columns]
    version_column = _sanitize_sql(version_column)

    q_str = f'''
        SELECT DISTINCT ON({', '.join(key_columns)})
            {select_columns}
        FROM {table}
        {f'WHERE {pairs_query}' if pairs_query else ''}
        ORDER BY {', '.join(key_columns)}, {version_column} DESC
    '''
    with session.begin() as tx:
        # only keep the first and last values of each key
        mp_sql_params = {f'{i}_{k}': v for i, k, c, v in mp_indexed}
        # print(sql(q_str).bindparams(**mp_sql_params).compile(
        #     dialect=postgresql.dialect(),
        #     compile_kwargs={"literal_binds": True}
        # ))
        data = tx.execute(sql(q_str).bindparams(**mp_sql_params)).all()

    return data