from dataclasses import dataclass
from typing import Literal

import pandas as pd

from orcha.core.module_base import DatabaseEntity
from orcha.utils.sqlalchemy import postgres_scaffold, postgres_upsert


@dataclass
class PostgresEntity(DatabaseEntity):

    def __init__(
            self, module_idk: str, name: str, description: str,
            user_name: str, password: str, host: str, port: int,
            database_name: str, schema_name: str
    ):
        super().__init__(
            module_idk=module_idk,
            name=name,
            description=description,
            user_name=user_name,
            password=password,
            host=host,
            port=port,
            database_name=database_name,
            schema_name=schema_name
        )
        self.declarative_base, self.engine, self.sessionmaker = postgres_scaffold(
            user=user_name,
            passwd=password,
            server=host,
            db=database_name,
            schema=schema_name
        )


    def to_sql(
            self, data: pd.DataFrame, table_name: str,
            if_exists: Literal['fail', 'replace', 'append', 'upsert'] = 'fail',
            index: bool = False, **kwargs
        ) -> None:
        """
        This is a wrapper around pd.to_sql except where 'upsert' is used
        and database specific upserts are used
        """
        if self.declarative_base is None:
            raise Exception('No declarative base set')
        elif self.sessionmaker is None:
            raise Exception('No sessionmaker set')
        elif self.engine is None:
            raise Exception('No engine set')
        elif if_exists == 'upsert':
            return postgres_upsert(
                base=self.declarative_base,
                session=self.sessionmaker,
                table=table_name,
                data=data
            )

        data.to_sql(table_name, self.engine, if_exists=if_exists, index=index, **kwargs)


