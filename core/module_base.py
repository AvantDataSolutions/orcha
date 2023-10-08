from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from abc import ABC, abstractmethod


import pandas as pd
from sqlalchemy.engine.mock import MockConnection
from sqlalchemy.orm import sessionmaker

from orcha.utils.sqlalchemy import DeclarativeBaseType

def module_function(func):
    """
    Decorator for module functions that will catch any exceptions and
    raise them with the relevant module information
    """
    def wrapper(module_base: ModuleBase, *args, **kwargs):
        try:
            return func(module_base, *args, **kwargs)
        except Exception as e:
            raise Exception(f'Exception in {module_base.name} ({module_base.module_idk}) module: {e}')
    return wrapper


@dataclass
class ModuleBase(ABC):
    module_idk: str
    name: str
    description: str


@dataclass
class EntityBase(ModuleBase):
    user_name: str
    password: str


@dataclass
class DatabaseEntity(EntityBase):
    host: str
    port: int
    database_name: str
    schema_name: str
    declarative_base: DeclarativeBaseType | None = None
    engine: MockConnection | None = None
    sessionmaker: sessionmaker | None = None

    def run_query(self, query: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f'{__class__.__name__} does not implement run_query')

    def read_sql(self, query: str, **kwargs) -> pd.DataFrame:
        """
        This is a wrapper around pd.read_sql
        """
        if self.engine is None:
            raise Exception('No engine set')
        return pd.read_sql(query, self.engine, **kwargs)

    def to_sql(
            self, data: pd.DataFrame, table_name: str,
            if_exists: Literal['fail', 'replace', 'append', 'upsert'] = 'fail',
            index: bool = False, **kwargs
        ) -> None:
        """
        This is a wrapper around pd.to_sql except where 'upsert' is used
        and database specific upserts are used
        """
        if if_exists == 'upsert':
            raise NotImplementedError(f'{__class__.__name__} does not implement upsert')
        elif self.engine is None:
            raise Exception('No engine set')
        data.to_sql(table_name, self.engine, if_exists=if_exists, index=index, **kwargs)


class RestEntity(EntityBase):
    url: str


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
    query: str


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
    table_name: str
    if_exists: Literal['fail', 'replace', 'append', 'upsert'] = 'fail'
    index: bool = False

    def save(self, data: pd.DataFrame, **kwargs) -> None:
        self.data_entity.to_sql(
            data=data,
            table_name=self.table_name,
            if_exists=self.if_exists,
            index=self.index,
            **kwargs
        )


@dataclass
class TransformBase(ModuleBase):
    @staticmethod
    @module_function
    def transform(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f'{__class__.__name__} does not implement transform')


@dataclass
class ValidationBase(ModuleBase):
    @staticmethod
    @module_function
    def validate(data: pd.DataFrame, **kwargs) -> bool:
        raise NotImplementedError(f'{__class__.__name__} does not implement validate')


@dataclass
class PipelineBase(ModuleBase):
    not_implemented = NotImplementedError('This method is not implemented')