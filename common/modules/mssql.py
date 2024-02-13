from dataclasses import dataclass
from typing import Literal

import pandas as pd
from orcha.core.module_base import DatabaseEntity
from orcha.utils.sqlalchemy import (
    mssql_partial_scaffold,
    mssql_upsert,
    sqlalchemy_replace,
)
from sqlalchemy import Table


@dataclass
class MssqlEntity(DatabaseEntity):

    def __init__(
            self, module_idk: str, name: str, description: str,
            user_name: str, password: str, host: str, port: int,
            database_name: str
    ):
        super().__init__(
            module_idk=module_idk,
            description=description,
            user_name=user_name,
            password=password,
            host=host,
            port=port,
            database_name=database_name
        )
        self.engine, self.sessionmaker = mssql_partial_scaffold(
            user=user_name,
            passwd=password,
            server=host,
            db=database_name
        )


    def to_sql(
            self, data: pd.DataFrame, table: Table,
            if_exists: Literal['fail', 'replace', 'delete_replace', 'append', 'upsert'] = 'fail',
            index: bool = False, **kwargs
        ) -> None:
        """
        This is a wrapper around pd.to_sql except where 'upsert' is used
        and database specific upserts are used
        kwargs: collation will be passed to upsert if provided
        """
        if table not in self._tables:
            raise Exception('Table not defined in this entity')
        elif self.sessionmaker is None:
            raise Exception('No sessionmaker set')
        elif self.engine is None:
            raise Exception('No engine set')
        elif if_exists == 'upsert':
            return mssql_upsert(
                s_maker=self.sessionmaker,
                table=table,
                data=data
            )
        elif if_exists == 'delete_replace':
            return sqlalchemy_replace(
                session=self.sessionmaker,
                table=table,
                data=data
            )

        data.to_sql(table.name, self.engine, if_exists=if_exists, index=index, schema=table.schema, **kwargs)
