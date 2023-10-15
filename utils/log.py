from __future__ import annotations

from datetime import datetime as dt
from uuid import uuid4

from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.postgresql import JSON as SQL_JSON
from sqlalchemy.dialects.postgresql import UUID as SQL_UUID
from orcha.utils.sqlalchemy import sqlalchemy_build, postgres_scaffold


class LogManager:

    @staticmethod
    def setup_sqlalchemy(
            user: str, passwd: str,
            server: str, db: str
        ):
        """
        This function must be called before any other functions in the orcha package.
        This function does the following:
        - Sets up the sqlalchemy database connection
        """
        CUR_SCHEMA = 'orcha_logs'
        global Base, engine, Session, LogEntryRecord
        Base, engine, Session = postgres_scaffold(
            user=user,
            passwd=passwd,
            server=server,
            db=db,
            schema=CUR_SCHEMA
        )

        class LogEntryRecord(Base):
            __tablename__ = 'logs'
            created = Column(DateTime)
            id = Column(SQL_UUID(as_uuid=True), primary_key=True)
            actor = Column(String)
            source = Column(String)
            category = Column(String)
            text = Column(String)
            json = Column(SQL_JSON)

        sqlalchemy_build(Base, engine, CUR_SCHEMA)


    def __init__(self, source_name: str) -> None:
        self.source = source_name


    def add_entry(self, actor: str, category: str, text: str, json: dict):
        with Session.begin() as db:
            # Using add for performance, we never update/merge
            # old log entries
            db.add(LogEntryRecord(
                created = dt.utcnow(),
                id = str(uuid4()),
                actor = actor,
                source = self.source,
                category = category,
                text = text,
                json = json
            ))
