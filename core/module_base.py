from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime as dt
from functools import wraps
from typing import Callable, Generic, Literal, TypeVar

import pandas as pd
from sqlalchemy import Column, Index, MetaData, Table, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import text as sql

from orcha.utils import kvdb
from orcha.utils.sqlalchemy import create_table


@dataclass
class ModuleConfig():
    """
    This class is used to store module configuration.
    ### Options
    - max_retries(int): The maximum number of retries to attempt.
    - retry_interval(int): The interval in seconds at which to retry.
    """
    max_retries: int = 1
    retry_interval: int = 10


GLOBAL_MODULE_CONFIG: ModuleConfig = ModuleConfig()
"""
Config used for all modules. Care should be taken when changing this
to make sure it is in scope for all modules intended to be affected.
"""


def module_function(func):
    """
    Decorator for module functions that will catch any exceptions and
    raise them with the relevant module information
    """
    @wraps(func)
    def wrapper(module_base: ModuleBase, *args, **kwargs):
        # Either use any retry config passed in or use the global one
        module_config = kwargs.get('module_config', GLOBAL_MODULE_CONFIG)
        # get the number of retries for this module, quietly passed via kwargs
        if not isinstance(module_config, ModuleConfig):
            Exception(f'Exception (ValueError) in {module_base.module_idk} ({module_base.module_idk}) module: module_config must be of type ModuleConfig')

        retry_count = 0
        retry_exceptions = []
        func_complete = False
        func_return_value = None

        # get the current run times to be updated later
        current_run_times = kvdb.get(
            key='current_run_times', as_type=list,
            storage_type='local'
        )
        if current_run_times is None:
            current_run_times = []

        start_time = dt.now()
        while retry_count <= module_config.max_retries and not func_complete:
            try:
                func_return_value = func(module_base, *args, **kwargs)
                end_time = dt.now()
                # if the run was successful then update the current run times
                # and set the function complete flag to break the loop
                duration = (end_time - start_time).total_seconds()
                current_run_times.append({
                    'module_idk': module_base.module_idk,
                    'start_time_posix': start_time.timestamp(),
                    'end_time_posix': end_time.timestamp(),
                    'duration_seconds': round(duration, 3),
                    'retry_count': retry_count,
                    'retry_exceptions': retry_exceptions,
                })
                func_complete = True
            except Exception as e:
                # if the function failed then we should retry
                # increment the retry count and record the exception
                retry_count = retry_count + 1
                retry_exceptions.append(str(e))

                # if we're on the last retry then we should raise the exception
                if retry_count > module_config.max_retries:
                    raise Exception(
                        f'Exception ({type(e).__name__}) in \
                        {module_base.module_idk} module: {e} \
                        (total attempts: {retry_count + 1})'
                    ) from e
                else:
                    # if we're not on the last retry then we should wait
                    time.sleep(module_config.retry_interval)

        # save the updated run times back to the kvdb
        kvdb.store(
            storage_type='local',
            key='current_run_times',
            value=current_run_times
        )
        # return the value from the function
        return func_return_value

    return wrapper


@dataclass
class ModuleBase():
    """
    This is the base class for all modules. It provides a consistent module_idk
    and description for all modules.
    """
    module_idk: str
    description: str
    # TODO Some form of lineage
    # upstream: list[ModuleBase]
    # downstream: list[ModuleBase]


@dataclass
class EntityBase(ModuleBase):
    """
    The base class for all entities which provides authentication
    details for sources and sinks to support convenient ways to
    swap in different credentials for dev, prod, etc.
    This is typically extended by a specific entity type; postgres,
    mysql, etc. to provide auth handling for that entity type.
    """
    user_name: str
    password: str


@dataclass
class DatabaseEntity(EntityBase):
    """
    The base class for all database entities which provides the
    connection details for the database which includes the host,
    port, and database name.
    """
    host: str
    port: int
    database_name: str
    engine: Engine | None = None
    sessionmaker: sessionmaker[Session] | None = None
    _tables: list[Table] = field(default_factory=list)

    def run_query(self, query: str, bindparams: dict, return_values: bool):
        """
        Execute a query on the database using the sessionmaker
        and return the results as a dataframe.
        ### Parameters:
        - query: The query to execute
        - bindparams: The bind parameters for the query
        - return_values: If True, the results will be returned as a dataframe
        otherwise None will be returned. This is used for non-returning queries.
        """
        if self.sessionmaker is None:
            raise Exception('No sessionmaker set')
        with self.sessionmaker.begin() as db:
            result = db.execute(sql(query).bindparams(**bindparams))
            if return_values:
                return pd.DataFrame(
                    [list(map(str, row)) for row in result.all()]
                )
            else:
                return None

    def load_table(self, schema_name: str, table_name: str):
        """
        Loads a table from the database into the entity. This is a shortcut
        for registering the table with the entity without having to define it.
        """
        if self.engine is None:
            raise Exception('No engine set')
        try:
            t = Table(
                table_name,
                MetaData(schema=schema_name),
                autoload_with=self.engine
            )
            self._tables.append(t)
            return t
        except Exception as e:
            raise Exception(
                f'Error loading table {table_name} from {schema_name} schema'
            ) from e

    def define_table(
            self, schema_name: str | None,  table_name: str,
            columns: list[Column] = [], indexes: list[Index] = [],
            build=True, match_definition=True
        ):
        """
        Define a table with the given name, columns, and primary key.
        build: If True, the table will be created in the database if it does not already exist and
            checks if the column names match. If False no column name checks will be done.
        """
        if self.engine is None:
            raise Exception('No engine set')

        table = create_table(
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            indexes=indexes,
            engine=self.engine,
            build_table=build,
            match_definition=match_definition
        )
        self._tables.append(table)
        return table

    def get_all_tables(self, schema_name: str) -> list[Table]:
        """
        Get all tables from the database
        """
        if self.engine is None:
            raise Exception('No engine set')

        tables = []
        inspector = inspect(self.engine)
        schemas = inspector.get_schema_names()

        for schema in schemas:
            if schema == schema_name or True:
                for table_name in inspector.get_table_names(schema=schema):
                    tables.append(Table(table_name, MetaData(schema=schema), autoload_with=self.engine))

        return tables

    def get_database_table(self, schema_name: str, table_name: str) -> Table:
        """
        Get a table from the database
        """
        if self.engine is None:
            raise Exception('No engine set')

        return Table(table_name, MetaData(schema=schema_name), autoload_with=self.engine)

    def read_sql(self, query: str, **kwargs) -> pd.DataFrame:
        """
        This is a wrapper around pd.read_sql
        """
        if self.engine is None:
            raise Exception('No engine set')
        return pd.read_sql(query, self.engine, **kwargs)

    def to_sql(
            self, data: pd.DataFrame, table: Table,
            if_exists: Literal['fail', 'replace', 'delete_replace', 'append', 'upsert'] = 'fail',
            index: bool = False, **kwargs
        ) -> None:
        """
        This is a wrapper around pd.to_sql except where 'upsert' is used
        and database specific upserts are used
        """
        if if_exists == 'upsert':
            raise NotImplementedError(f'{__class__.__name__} does not implement upsert')
        elif if_exists == 'delete_replace':
            raise NotImplementedError(f'{__class__.__name__} does not implement delete_replace')
        elif self.engine is None:
            raise Exception('No engine set')
        data.to_sql(table.name, self.engine, if_exists=if_exists, index=index, **kwargs)


class RestEntity(EntityBase):
    """
    The base class for all rest entities which provides the url.
    """
    url: str


class PythonEntity(EntityBase):
    """
    The base class for all python entities which provides the data.
    ### Attributes
    - credentials: A dictionary of arbitrary credentials to be used by the entity.
    """
    credentials: dict | None = None


@dataclass
class SourceBase(ModuleBase):
    """
    This is the base class for all sources. This is always extended
    by a specific source type; postgres, mysql, etc.
    While this class is abstract, it is not marked as such and will
    raise an exception if used directly.
    """
    data_entity: EntityBase | None

    def __init__(self, data_entity: EntityBase) -> None:
        """
        data_entity: The data entity to use for this source
        """
        raise NotImplementedError(f'{__class__.__name__} does not implement __init__')

    @module_function
    def get(self, **kwargs) -> pd.DataFrame:
        """
        The get method is used to get data from the source. Implementations
        are specific to the source type. Raises an exception if called directly
        on a SourceBase instance.
        """
        raise NotImplementedError(f'{__class__.__name__} does not implement get')


@dataclass
class DatabaseSource(SourceBase):
    """
    A generic source that executes arbitrary SQL queries
    to return any data that is required.
    Returns a dataframe.
    """
    data_entity: DatabaseEntity | None
    tables: list[Table]
    query: str

    @module_function
    def get(self, **kwargs) -> pd.DataFrame:
        """
        Get the data from the database using the query provided
        and calling the read_sql method on the data entity.
        """
        if self.data_entity is None:
            raise Exception('No data entity set for source')
        elif self.tables is None or self.tables == []:
            raise Exception('No tables set for source')
        else:
            return self.data_entity.read_sql(self.query, **kwargs)


@dataclass
class PythonSource(SourceBase):
    """
    A generic source that executes arbitrary python code
    to return any data that is required.
    Returns a dataframe.
    """
    data_entity: PythonEntity | None
    function: Callable[[PythonEntity], pd.DataFrame]

    @module_function
    def get(self, **kwargs) -> pd.DataFrame:
        """
        Calls the source function with the data entity
        as the first argument and any kwargs after that
        """
        if self.data_entity is None:
            raise Exception('No data entity set for source')
        else:
            return self.function(self.data_entity, **kwargs)


@dataclass
class SinkBase(ModuleBase):
    """
    This is the base class for all sinks. This is always extended
    by a specific sink type; postgres, mysql, etc.
    While this class is abstract, it is not marked as such and will
    raise an exception if used directly.
    """
    data_entity: EntityBase | None

    def __init__(self, data_entity: EntityBase) -> None:
        """
        data_entity: The data entity to use for this sink
        """
        raise NotImplementedError(f'{__class__.__name__} does not implement __init__')

    @module_function
    def save(self, data: pd.DataFrame, **kwargs) -> None:
        """
        The save method is used to save data to the sink. Implementations
        are specific to the sink type.
        """
        raise NotImplementedError(f'{__class__.__name__} does not implement save')


@dataclass
class DatabaseSink(SinkBase):
    data_entity: DatabaseEntity
    table: Table
    if_exists: Literal['fail', 'replace', 'delete_replace', 'append', 'upsert'] = 'fail'
    index: bool = False

    @module_function
    def save(self, data: pd.DataFrame, **kwargs) -> None:
        """
        Save the data to the database using the to_sql method
        provided by the data entity.
        """
        if self.data_entity is None:
            raise Exception('No data entity set for sink')
        elif self.table is None:
            raise Exception('No table set for sink')

        self.data_entity.to_sql(
            data=data,
            table=self.table,
            if_exists=self.if_exists,
            index=self.index,
            **kwargs
        )

# Define the type of the inputs for Transformers and Validations
T = TypeVar('T')

@dataclass
class TransformBase(ModuleBase, Generic[T]):
    transform_func: Callable[[T], pd.DataFrame]
    create_inputs: type[T]

    @module_function
    def transform(self, inputs:T, **kwargs) -> pd.DataFrame:
        """
        Perform a transformation on the inputs and return a dataframe.
        This is a wrapper around the transform_func provided and
        create_inputs is used to ensure the correct type of inputs.
        This is enables logging of module run times and retries for
        long or error-prone transformations.
        """
        return self.transform_func(inputs)


@dataclass
class ValidationBase(ModuleBase, Generic[T]):
    validate_func: Callable[[pd.DataFrame, T], bool]
    create_inputs: type[T]

    @module_function
    def validate(self, data: pd.DataFrame, inputs:T, **kwargs) -> bool:
        """
        Perform a validation on the data and inputs and return a boolean.
        This is a wrapper around the validate_func provided and
        create_inputs is used to ensure the correct type of inputs.
        This is primarily for re-use of custom validation functions.
        """
        return self.validate_func(data, inputs, **kwargs)


@dataclass
class PipelineBase(ModuleBase):
    """
    Not implemented yet
    """
    not_implemented = NotImplementedError('This method is not implemented')