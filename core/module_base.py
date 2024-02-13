from __future__ import annotations

import time
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import datetime as dt
from typing import Callable, Literal

import pandas as pd
from sqlalchemy import Column, Index, Table
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


GLOBAL_MODULE_CONFIG: ModuleConfig | None = ModuleConfig()
"""
Config used for all modules. Care should be taken when changing this
to make sure it is in scope for all modules intended to be affected.
"""


def module_function(func):
    """
    Decorator for module functions that will catch any exceptions and
    raise them with the relevant module information
    """
    def wrapper(module_base: ModuleBase, *args, **kwargs):
        # Either use any retry config passed in or use the global one
        module_config = kwargs.get('module_config', GLOBAL_MODULE_CONFIG)
        if not isinstance(module_config, ModuleConfig):
            Exception(f'Exception (ValueError) in {module_base.module_idk} ({module_base.module_idk}) module: module_config must be of type ModuleConfig')
        try:
            start_time = dt.now()
            return_value = func(module_base, *args, **kwargs)
            end_time = dt.now()
            # get any existing runs
            # if we're running outside of the scheduler
            # then we won't have a key in the kvdb and will return None
            current_run_times = kvdb.get('current_run_times', list, 'local')
            if current_run_times is None:
                current_run_times = []
            # get the number of retries for this module
            # using get/setattr to quietly manage temp variables for the module
            # between retries
            retry_count = getattr(module_base, '_mfcg_wrapper_attempts', 0)
            # then add the new run time to it. We shouldn't have to
            # deal with concurrent access here as each module is
            # is run in the same thread
            current_run_times.append({
                'module_idk': module_base.module_idk,
                'module_name': module_base.name,
                'start_time_posix': start_time.timestamp(),
                'end_time_posix': end_time.timestamp(),
                'duration_seconds': (end_time - start_time).total_seconds(),
                'retry_count': retry_count
            })
            kvdb.store('local', 'current_run_times', current_run_times)
            return return_value
        except Exception as e:
            total_attempts = getattr(module_base, '_mfcg_wrapper_attempts', 0) + 1
            if total_attempts > module_config.max_retries:
                raise Exception(f'Exception ({type(e).__name__}) in {module_base.name} ({module_base.module_idk}) module: {e} (total attempts: {total_attempts})')
            else:
                setattr(module_base, '_mfcg_wrapper_attempts', total_attempts)
                time.sleep(module_config.retry_interval)
                return wrapper(module_base, *args, **kwargs)
    return wrapper


@dataclass
class ModuleBase():
    module_idk: str
    description: str
    # TODO Some form of lineage
    # upstream: list[ModuleBase]
    # downstream: list[ModuleBase]


@dataclass
class EntityBase(ModuleBase):
    user_name: str
    password: str


@dataclass
class DatabaseEntity(EntityBase):
    host: str
    port: int
    database_name: str
    engine: Engine | None = None
    sessionmaker: sessionmaker[Session] | None = None
    _tables: list[Table] = field(default_factory=list)

    def run_query(self, query: str, bindparams: dict, return_values: bool):
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

    def define_table(
            self, schema_name: str,  table_name: str,
            columns: list[Column], indexes: list[Index] = [],
            build=True
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
            build_table=build
        )
        self._tables.append(table)
        return table


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
    url: str


class PythonEntity(EntityBase):
    credentials: dict | None = None


@dataclass
class SourceBase(ModuleBase):
    data_entity: EntityBase | None

    def __init__(self, data_entity: EntityBase) -> None:
        """
        data_entity: The data entity to use for this source
        """
        raise NotImplementedError(f'{__class__.__name__} does not implement __init__')

    @module_function
    def get(self, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f'{__class__.__name__} does not implement get')


@dataclass
class DatabaseSource(SourceBase):
    data_entity: DatabaseEntity | None
    tables: list[Table]
    query: str

    @module_function
    def get(self, **kwargs) -> pd.DataFrame:
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
    data_entity: EntityBase | None

    def __init__(self, data_entity: EntityBase) -> None:
        """
        data_entity: The data entity to use for this sink
        """
        raise NotImplementedError(f'{__class__.__name__} does not implement __init__')

    @module_function
    def save(self, data: pd.DataFrame, **kwargs) -> None:
        raise NotImplementedError(f'{__class__.__name__} does not implement save')


@dataclass
class DatabaseSink(SinkBase):
    data_entity: DatabaseEntity
    table: Table
    if_exists: Literal['fail', 'replace', 'delete_replace', 'append', 'upsert'] = 'fail'
    index: bool = False

    @module_function
    def save(self, data: pd.DataFrame, **kwargs) -> None:

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


class TransformBase(ModuleBase):
    @staticmethod
    @abstractmethod
    def transform(data: pd.DataFrame, **kwargs):
        return NotImplementedError(f'{__class__.__name__} does not implement transform')


@dataclass
class ValidationBase(ModuleBase):
    function: Callable
    @module_function
    def validate(self, data: pd.DataFrame, **kwargs) -> bool:
        return self.function(data, **kwargs)


@dataclass
class PipelineBase(ModuleBase):
    not_implemented = NotImplementedError('This method is not implemented')