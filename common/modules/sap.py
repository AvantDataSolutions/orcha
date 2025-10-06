import pandas as pd
from dataclasses import dataclass
from hdbcli import dbapi

from pandas.core.api import DataFrame as DataFrame
from sqlalchemy import Table, create_engine, text
from sqlalchemy.orm import sessionmaker

from orcha.core.module_base import DatabaseEntity

@dataclass
class SAPEntity(DatabaseEntity):
    def __init__(
            self, module_idk: str, description: str,
            host: str, port: int, user_name: str, password: str,
            default_schema: str | None = None
    ):
        """
        Initializes the SAPEntity with the given parameters. This entity uses
        the HANA database driver (hdbcli) to connect to an SAP HANA database.
        #### NOTE: Due to the complexity of SAP, this entity is read-only \
        and does not validate tables or column types. Errors will be raised \
        at runtime if there are issues connecting or querying.
        """
        super().__init__(
            module_idk=module_idk,
            description=description,
            user_name=user_name,
            password=password,
            host=host,
            port=port,
            database_name='',
        )

        self.default_schema = default_schema

        url = f"hana://{user_name}:{password}@{host}:{port}"

        # create engine and sessionmaker
        self.engine = create_engine(
            url,
            future=True,
            pool_pre_ping=True,
            echo=False,
        )

        self.sessionmaker = sessionmaker(bind=self.engine, expire_on_commit=False)

    def read_sql(self, query: str, **kwargs) -> DataFrame:
        if not self.engine:
            raise Exception('No engine set')
        conn = self.engine.connect()
        if self.default_schema:
            conn.execute(text(f'SET SCHEMA {self.default_schema}'))
        result = pd.read_sql(text(query), conn, **kwargs)
        conn.close()
        return result


    def to_sql(self, kwargs) -> None:
        raise NotImplementedError("SAPEntity is read-only and does not support to_sql.")