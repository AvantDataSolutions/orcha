from __future__ import annotations

import re
from secrets import token_hex
from typing import Literal

import pandas as pd
from sqlalchemy import (
    TIMESTAMP,
    Column,
    DateTime,
    Index,
    MetaData,
    Table,
    create_engine,
    delete,
    inspect,
)
from sqlalchemy import insert as sqla_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.mssql import NVARCHAR
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from sqlalchemy.engine.row import Row
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Session, sessionmaker, declarative_base
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql import text as sql

_SCAFFOLD_CACHE: dict[str, tuple[DeclarativeMeta, Engine, sessionmaker[Session]]] = {}

CHUNK_SIZE = 1000
"""
mssql has a limit of 1000 rows per insert statement
however we use this as a general limit for all databases
to keep performance and memory more consistent across
all database types
"""

def postgres_partial_scaffold(
        user: str,
        passwd: str,
        server: str,
        db: str,
        application_name: str
    ):
    """
    Creates a connection to a database without a schema or
    declarative base object. Returns the engine and sessionmaker.
    Postgres specific connection parameters are set here.
    """
    engine = create_engine(
        f'postgresql://{user}:{passwd}@{server}/{db}?application_name={application_name}',
        pool_size=50,
        max_overflow=2,
        pool_recycle=300,
        pool_use_lifo=True
    )
    session = sessionmaker(bind=engine, expire_on_commit=False)
    return engine, session


def postgres_scaffold(
        user: str,
        passwd: str,
        server: str,
        db: str,
        schema: str,
        application_name: str
    ):
    """
    Creates a connection to a specific database and schema,
    and returns the SQLAlchemy Base object, engine and sessionmaker.
    Postgres specific connection parameters are set here.
    """
    if schema not in _SCAFFOLD_CACHE:
        engine, session = postgres_partial_scaffold(
            user, passwd, server, db, application_name
        )
        Base = declarative_base(metadata=MetaData(schema=schema))

        _SCAFFOLD_CACHE[schema] = (Base, engine, session)
    if schema in _SCAFFOLD_CACHE:
        return _SCAFFOLD_CACHE[schema]
    else:
        raise Exception('Failed to create scaffold for: ' + schema)


def mssql_partial_scaffold(user: str, passwd: str, server: str, db: str):
    """
    Creates a connection to a database without a schema or
    declarative base object. Returns the engine and sessionmaker.
    Postgres specific connection parameters are set here.
    """
    engine = create_engine(
        f'mssql+pymssql://{user}:{passwd}@{server}/{db}'
    )
    session = sessionmaker(bind=engine)
    return engine, session


def sqlite_partial_scaffold(db_path: str):
    """
    Creates a connection to a SQLite database without a schema or
    declarative base object. Returns the engine and sessionmaker.
    SQLite specific connection parameters are set here.
    """
    engine = create_engine(f'sqlite:///{db_path}')
    session = sessionmaker(bind=engine)
    return engine, session


def sqlalchemy_build(base: DeclarativeMeta, engine: Engine, schema_name: str):
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


def sqlalchemy_build_schema(schema_name: str, engine: Engine):
    """
    Creates a schema in the database if it does not already exist.
    """
    engine_inspect = inspect(engine)
    if engine_inspect is None:
        raise Exception('Engine inspect failed for schema: ' + schema_name)
    if schema_name not in engine_inspect.get_schema_names():
        with engine.begin() as db:
            db.execute(CreateSchema(schema_name))


def sqlalchemy_build_table(table: Table, engine: Engine):
    """
    Creates a table in the database if it does not already exist.
    """
    table.create(engine, checkfirst=True)


def check_definitions(table: Table, engine: Engine, match_indexes: bool = True):
    """
    Loads the existing table from the database and checks that the
    provided table definition matches that in the database.
    Raises an exception if the definitions do not match or returns True for convenience.
    """
    engine_inspect = inspect(engine)
    if engine_inspect is None:
        raise Exception('Engine inspect failed for table: ' + table.name)
    # If the table exists, check that the columns match
    existing_table = Table(table.name, MetaData(schema=table.schema), autoload_with=engine)
    table_match, match_str = tables_match(table, existing_table)
    if not table_match:
        raise Exception(f'Table "{table}" does not match the database table defintion: {match_str}')
    # If the table exists, check that the indexes match
    if match_indexes:
        index_match, match_str = indexes_match(table, existing_table)
        if not index_match:
            raise Exception(f'Table "{table}" does not match the database table defintion: {match_str}')
    return True

def indexes_match(table1, table2):
    """
    Checks if the indexes of two SQLAlchemy tables match.
    """
    if len(table1.indexes) != len(table2.indexes):
        return False, 'Index count mismatch'
    for index1 in table1.indexes:
        index2 = None
        for index in table2.indexes:
            if index.name == index1.name:
                index2 = index
                break

        if index2 is None:
            return False, f'Index {index1.name} not in {table2.name}'

        index1_cols = [col.name for col in index1.columns]
        index2_cols = [col.name for col in index2.columns]
        for col_name in index1_cols:
            if col_name not in index2_cols:
                return False, f'Index column mismatch: definition columns {index1_cols}, database columns {index2_cols}'

    return True, ''


def tables_match(table1, table2):
    """
    Checks if the column names and types of two SQLAlchemy tables match.
    """
    t1_missing_names = set(table1.columns.keys()) - set(table2.columns.keys())
    t2_missing_names = set(table2.columns.keys()) - set(table1.columns.keys())

    if len(t1_missing_names) > 0:
        return False, f'Table {table1.name} is missing columns: {t1_missing_names}'
    if len(t2_missing_names) > 0:
        return False, f'Table {table2.name} is missing columns: {t2_missing_names}'

    for column1 in table1.columns:
        column2 = table2.columns[column1.name]
        # DateTime and TIMESTAMP are functionally equivalent so mark them as equal
        if isinstance(column1.type, DateTime) and isinstance(column2.type, TIMESTAMP):
            continue
        if isinstance(column2.type, DateTime) and isinstance(column1.type, TIMESTAMP):
            continue
        # Make sure column types match exactly, this includes length,
        # precision, collation, etc. Must compare text representations
        # of the types as the types themselves compare to false.
        if str(column1.type) != str(column2.type):
            return False, f'Column {column1.name} type mismatch: {column1.type} != {column2.type}'
    return True, ''


def create_table(
        schema_name: str | None,  table_name: str,
        engine: Engine,
        columns: list[Column], indexes: list[Index] = [],
        match_definition: bool = True,
        match_indexes: bool = True,
        build_table: bool = True
    ):
    """
    Returns an SQLAlchemy Table object with the given name, column definitions.
    Optionally can build this table in the database or not.
    """
    # Typically for sqlite which doesnt use schemas
    if schema_name is None:
        metadata = MetaData()
    else:
        metadata = MetaData(schema=schema_name)
    table = Table(
        table_name,
        metadata,
        *columns,
        *indexes
    )
    if build_table:
        sqlalchemy_build_table(table, engine)
    if match_definition:
        check_definitions(table, engine, match_indexes)

    return table


def sqlalchemy_replace(
        session: sessionmaker, table: Table, data: pd.DataFrame
    ):
    """
    Replaces the data in a table with the data in a dataframe.
    Uses the session to execute the replace statement and chunk the data
    to reduce memory usage.
    """
    with session.begin() as db:
        delete_stmt = delete(table)
        db.execute(delete_stmt)
        # Chunk data to reduce peak memory usage
        # when converting large dataframes to rows
        for chunk in range(0, len(data), CHUNK_SIZE):
            rows = data.iloc[chunk:chunk+CHUNK_SIZE].to_dict('records')
            # Bug with pymssql when handling nan values, need to convert them to None
            # https://stackoverflow.com/questions/52862703/error-while-inserting-records-with-null-values-into-sql-server-using-pymssql
            for r in rows:
                for k, v in r.items():
                    if pd.isna(v) and v is not None:
                        r[k] = None
            insert_stmt = sqla_insert(table).values(rows)
            db.execute(insert_stmt)


def postgres_upsert(
        session: sessionmaker, table: Table, data: pd.DataFrame
    ) -> None:
    """
    Performs an upsert operation on a PostgreSQL table using
    on_conflict_do_update. This is done in chunks to reduce memory
    usage when upserting large dataframes.
    """
    if len(data) == 0:
        return None

    with session.begin() as db:
        table_inspect = inspect(table)
        if table_inspect is None:
            return None
        index_elements = [column.name for column in table_inspect.primary_key]
        if len(index_elements) == 0:
            raise Exception('Cannot upsert on table with no Primary Key')
        for chunk in [data[i:i+CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]:
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


def sqlite_upsert(
        session: sessionmaker, table: Table, data: pd.DataFrame
    ) -> None:
    """
    Performs an upsert operation on a SQLite table using
    OR REPLACE. This is done in chunks to reduce memory
    usage when upserting large dataframes.
    """
    if len(data) == 0:
        return None

    with session.begin() as db:
        table_inspect = inspect(table)
        if table_inspect is None:
            return None
        index_elements = [column.name for column in table_inspect.primary_key]
        if len(index_elements) == 0:
            raise Exception('Cannot upsert on table with no Primary Key')
        for chunk in [data[i:i+CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]:
            stmt = sqlite_insert(table).values(chunk.to_dict('records'))
            stmt = stmt.prefix_with('OR REPLACE')
            db.execute(stmt)


def mssql_upsert(
        data: pd.DataFrame,
        s_maker: sessionmaker[Session],
        table: Table
    ):
    """
    Performs an upsert operation on a MSSQL table using
    MERGE. This is done in chunks to reduce memory
    usage when upserting large dataframes.
    A temporary table is created to hold the data and then
    merged with the target table.
    """
    if len(data) == 0:
        return None
    with s_maker.begin() as session:
        conn = session.connection()
        table_inspect = inspect(table)
        if table_inspect is None:
            return None

        temp_table = f'#temp_{token_hex(16)}'
        merge_on = [column.name for column in table_inspect.primary_key]
        non_pk_cols = [
            c.name
            for c in table_inspect.columns if c.name not in merge_on
        ]
        # Remove any columns that aren't in the data to avoid column
        # name errors; e.g. __valid_to in temporal tables
        non_pk_cols = [c for c in non_pk_cols if c in data.columns]

        if len(merge_on) == 0:
            raise Exception('Cannot upsert on table with no Primary Key')

        schema_str = str(table.schema)
        if schema_str is None:
            raise Exception('Table schema is None')
        if schema_str[0] != '[':
            schema_str = '[' + schema_str
        if schema_str[-1] != ']':
            schema_str = schema_str + ']'

        dtype_map = {}
        for col in data.columns:
            # treat object/string dtypes as text; pick length from table definition if available,
            # otherwise use the max length observed in the data (or 200 as a safe default)
            if data[col].dtype == object or str(data[col].dtype).startswith('string'):
                try:
                    col_def = table_inspect.columns[col]
                    length = getattr(col_def.type, 'length', None)
                except Exception:
                    length = None
                if length is None or length <= 0:
                    try:
                        sample_max = int(data[col].astype(str).str.len().max())
                        length = max(1, min(4000, sample_max or 200))
                    except Exception:
                        length = 200
                dtype_map[col] = NVARCHAR(length)


        data.to_sql(
            name=temp_table,
            schema=schema_str,
            con=conn,
            method='multi',
            chunksize=CHUNK_SIZE,
            index=False,
            dtype=dtype_map
        )

        # Just in case the database collation is different to server collation
        # and then the temp table won't merge with the target table
        # we have to fix the collation of the temp table
        db_name = conn.engine.url.database
        collation = session.execute(sql(f'''
            SELECT CAST(DATABASEPROPERTYEX('{db_name}', 'Collation') AS VARCHAR) AS DatabaseCollation;
        ''')).fetchone()
        if collation is None or not hasattr(collation, 'DatabaseCollation'):
            raise Exception('Failed to get database collation')

        collation = collation.DatabaseCollation

        def _needs_collation(column_type: str) -> bool:
            collatable_types = ['varchar', 'char', 'text', 'nchar', 'nvarchar', 'ntext']
            for collatable_type in collatable_types:
                if collatable_type in column_type.lower():
                    return True
            return False

        def _alreay_has_collation(column_type: str) -> bool:
            if 'collate' in column_type.lower():
                return True
            return False

        for column in data.columns:
            column_type = str(table_inspect.columns[column].type)
            # Mostly to handle cases where server collation != db collation
            if _needs_collation(column_type):
                # If the column type doesn't already have a collation, then use
                # the database collation
                if not _alreay_has_collation(column_type):
                    session.execute(sql(f'''
                        ALTER TABLE {temp_table}
                        ALTER COLUMN [{column}] {column_type} COLLATE {collation}
                    '''))
                else:
                    # otherwise we'll use whatever collation is specified in the
                    # table column type and remove any extra "
                    session.execute(sql(f'''
                        ALTER TABLE {temp_table}
                        ALTER COLUMN [{column}] {column_type.replace('"', '')}
                    '''))
        session.execute(sql(f'''
            MERGE {schema_str}.{table.name} WITH (HOLDLOCK, UPDLOCK) AS target
            USING {temp_table} AS source
            ON (
                {' AND '.join(f'source.[{c}] = target.[{c}]' for c in merge_on)}
            )
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({', '.join([f'[{c}]' for c in data.columns])})
                VALUES ({', '.join(f'source.[{c}]' for c in data.columns)})
            WHEN MATCHED THEN UPDATE SET
                {', '.join(f'target.[{c}] = source.[{c}]' for c in non_pk_cols)};
            '''))
        session.execute(sql(f'DROP TABLE {temp_table}'))


def _sanitize_sql(input_str: str) -> str:
    # drop ; and --
    sanitized_str = re.sub(r';|--', '', input_str)
    # If the string has changed, call the function again
    if sanitized_str != input_str:
        return _sanitize_sql(sanitized_str)
    # Return the sanitized string
    return sanitized_str


def _split_table_name(table_name: str) -> tuple[str, str]:
    if '.' not in table_name:
        raise Exception('Table name must be in the format: schema.table')
    if '].[' in table_name:
        split = table_name.split('].[')
        split[0] = split[0] + ']'
        split[1] = '[' + split[1]
    elif '].' in table_name:
        split = table_name.split('].')
        split[0] = split[0] + ']'
    elif '.[' in table_name:
        split = table_name.split('.[')
        split[1] = '[' + split[1]
    else:
        split = table_name.split('.')
    if len(split) != 2:
        raise Exception('Table name must be in the format: schema.table')

    return (f'{split[0]}', f'{split[1]}')


def get(
        s_maker: sessionmaker[Session],
        table: str,
        select_columns: list | Literal['*'],
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
    schema, table_name = _split_table_name(table)
    table_name = _sanitize_sql(table_name)
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
        FROM {schema}.{table_name}
        {f'WHERE {pairs_query}' if pairs_query else ''}
    '''
    with s_maker.begin() as session:
        # only keep the first and last values of each key
        mp_sql_params = {f'{i}_{k}': v for i, k, c, v in mp_indexed}
        # print(sql(q_str).bindparams(**mp_sql_params).compile(
        #     dialect=postgresql.dialect(),
        #     compile_kwargs={"literal_binds": True}
        # ))
        return list(session.execute(sql(q_str).bindparams(**mp_sql_params)).all())



def get_latest_versions(
        s_maker: sessionmaker[Session],
        table: str,
        key_columns: list, version_column: str,
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
    with s_maker.begin() as tx:
        # only keep the first and last values of each key
        mp_sql_params = {f'{i}_{k}': v for i, k, c, v in mp_indexed}
        # print(sql(q_str).bindparams(**mp_sql_params).compile(
        #     dialect=postgresql.dialect(),
        #     compile_kwargs={"literal_binds": True}
        # ))
        data = tx.execute(sql(q_str).bindparams(**mp_sql_params)).all()

    return list(data)