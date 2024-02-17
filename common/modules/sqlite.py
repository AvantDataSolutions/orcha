from dataclasses import dataclass
from typing import Literal

import pandas as pd
from sqlalchemy import Table

from orcha.core.module_base import DatabaseEntity
from orcha.utils.sqlalchemy import (
    sqlalchemy_replace,
    sqlite_partial_scaffold,
    sqlite_upsert,
)


@dataclass
class SQLiteEntity(DatabaseEntity):

    def __init__(
            self, module_idk: str, description: str,
            database_path: str
    ):
        super().__init__(
            module_idk=module_idk,
            description=description,
            user_name='',
            password='',
            host='',
            port=0,
            database_name=database_path
        )
        self.engine, self.sessionmaker = sqlite_partial_scaffold(
            db_path=database_path
        )


    def to_sql(
            self, data: pd.DataFrame, table: Table,
            if_exists: Literal['fail', 'replace', 'delete_replace', 'append', 'upsert'] = 'fail',
            index: bool = False, **kwargs
        ) -> None:
        """
        This is a wrapper around pd.to_sql except where 'upsert' is used
        and database specific upserts are used
        """
        if table not in self._tables:
            raise Exception('Table not defined in this entity')
        elif self.sessionmaker is None:
            raise Exception('No sessionmaker set')
        elif self.engine is None:
            raise Exception('No engine set')
        elif if_exists == 'upsert':
            return sqlite_upsert(
                session=self.sessionmaker,
                table=table,
                data=data
            )
        elif if_exists == 'delete_replace':
            return sqlalchemy_replace(
                session=self.sessionmaker,
                table=table,
                data=data
            )

        data.to_sql(table.name, self.engine, if_exists=if_exists, index=index, **kwargs)
