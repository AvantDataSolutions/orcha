from __future__ import annotations

import re
from typing import Literal

import pandas as pd
from sqlalchemy import (
    MetaData, Table, Column, create_engine, inspect,
    delete, insert as sqla_insert
)
from sqlalchemy import DateTime, TIMESTAMP
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine.mock import MockConnection
from sqlalchemy.engine.row import Row
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql import text as sql


SCAFFOLD_CACHE: dict[str, tuple[DeclarativeMeta, MockConnection, sessionmaker]] = {}

def postgres_partial_scaffold(user: str, passwd: str, server: str, db: str):
    """
    Creates a connection to a database without a schema or
    declarative base object. Returns the engine and sessionmaker.
    Postgres specific connection parameters are set here.
    """
    engine = create_engine(
        f'postgresql://{user}:{passwd}@{server}/{db}',
        pool_size=50,
        max_overflow=2,
        pool_recycle=300,
        pool_use_lifo=True
    )
    session = sessionmaker(bind=engine)
    return engine, session

def postgres_scaffold(user: str, passwd: str, server: str, db: str, schema: str):
    """
    Creates a connection to a specific database and schema,
    and returns the SQLAlchemy Base object, engine and sessionmaker.
    Postgres specific connection parameters are set here.
    """
    if schema not in SCAFFOLD_CACHE:
        engine, session = postgres_partial_scaffold(user, passwd, server, db)
        Base = declarative_base(metadata=MetaData(schema=schema, bind=engine))

        SCAFFOLD_CACHE[schema] = (Base, engine, session)
    if schema in SCAFFOLD_CACHE:
        return SCAFFOLD_CACHE[schema]
    else:
        raise Exception('Failed to create scaffold for: ' + schema)


def sqlalchemy_build(base: DeclarativeMeta, engine: MockConnection, schema_name: str):
    """
    General build function for SQLAlchemy schema and tables.
    Uses the provided engine so is database agnostic.
    """
    sqlalchemy_build_schema(schema_name, engine)

    # Load the metadata from the existing database
    existing_metadata = MetaData()
    existing_metadata.reflect(bind=engine)

    # Compare the existing metadata to the metadata in your code
    for table_name, table in base.metadata.tables.items(): # type: ignore
        if table_name in existing_metadata.tables:
            if existing_metadata.tables is None:
                continue
            existing_table = existing_metadata.tables[table_name]
            if not tables_match(table, existing_table):
                raise Exception(f'Table {table} does not match the definition in the code.')


    base.metadata.create_all(engine, checkfirst=True) # type: ignore

def sqlalchemy_build_schema(schema_name: str, engine: MockConnection):
    engine_inspect = inspect(engine)
    if engine_inspect is None:
        raise Exception('Engine inspect failed for schema: ' + schema_name)
    if schema_name not in engine_inspect.get_schema_names():
        engine.execute(CreateSchema(schema_name))


def sqlalchemy_build_table(table: Table, engine: MockConnection):
    engine_inspect = inspect(engine)
    if engine_inspect is None:
        raise Exception('Engine inspect failed for table: ' + table.name)
    table.create(engine, checkfirst=True) # type: ignore
    # If the table exists, check that the columns match
    existing_table = Table(table.name, MetaData(schema=table.schema, bind=engine), autoload=True)
    if not tables_match(table, existing_table):
        raise Exception(f'Table "{table}" does not match the database table defintion')


def tables_match(table1, table2):
    """
    Checks if the column names and types of two SQLAlchemy tables match.
    """
    if len(table1.columns) != len(table2.columns):
        return False

    for column1 in table1.columns:
        if column1.name not in table2.columns:
            return False
        column2 = table2.columns[column1.name]
        # DateTime and TIMESTAMP are functionally equivalent so mark them as equal
        if isinstance(column1.type, DateTime) and isinstance(column2.type, TIMESTAMP):
            continue
        if isinstance(column2.type, DateTime) and isinstance(column1.type, TIMESTAMP):
            continue
        if str(column1.type) != str(column2.type):
            return False
    return True

def create_table(
        schema_name: str,  table_name: str,
        columns: list[Column], engine: MockConnection,
        build_table: bool = True
    ):
    """
    Returns an SQLAlchemy Table object with the given name, column definitions.
    Optionally can build this table in the database or not.
    """
    metadata = MetaData(schema=schema_name, bind=engine)
    table = Table(
        table_name,
        metadata,
        *columns
    )
    if build_table:
        sqlalchemy_build_table(table, engine)

    return table


def sqlalchemy_replace(
        session: sessionmaker, table: Table, data: pd.DataFrame
    ):

    with session.begin() as db:
        delete_stmt = delete(table)
        db.execute(delete_stmt)

        # Chunk data to reduce peak memory usage
        # when converting large dataframes to rows
        for chunk in range(0, len(data), 5000):
            rows = data.iloc[chunk:chunk+5000].to_dict('records')
            insert_stmt = sqla_insert(table).values(rows)
            db.execute(insert_stmt)


def postgres_upsert(
        session: sessionmaker, table: Table, data: pd.DataFrame
    ) -> None:
    if len(data) == 0:
        return None

    with session.begin() as db:
        table_inspect = inspect(table)
        if table_inspect is None:
            return None
        index_elements = [column.name for column in table_inspect.primary_key]
        if len(index_elements) == 0:
            raise Exception('Cannot upsert on table with no Primary Key')
        for chunk in [data[i:i+1000] for i in range(0, len(data), 1000)]:
            stmt = pg_insert(table).values(chunk.to_dict('records'))
            update_dict = {}
            for column in table_inspect.columns:
                if column.primary_key:
                    continue
                update_dict[column.name] = getattr(stmt.excluded, column.name)
            stmt = stmt.on_conflict_do_update(
                index_elements=index_elements,
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