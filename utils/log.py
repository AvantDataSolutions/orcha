from __future__ import annotations

from datetime import datetime as dt
from uuid import uuid4

from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.postgresql import JSON as SQL_JSON
from sqlalchemy.dialects.postgresql import UUID as SQL_UUID
from utils.sqlalchemy import postgres_build, postgres_scaffold

from orcha.core.credentials import *


class LogManager:

    CUR_SCHEMA = 'orcha'
    Base, engine, Session = postgres_scaffold(CUR_SCHEMA)


    class LogEntryRecord(Base):
        __tablename__ = 'logs'
        created = Column(DateTime)
        id = Column(SQL_UUID(as_uuid=True), primary_key=True)
        actor = Column(String)
        source = Column(String)
        category = Column(String)
        text = Column(String)
        json = Column(SQL_JSON)


    def __init__(self, source_name: str) -> None:
        self.source = source_name
        postgres_build(self.Base, self.engine, self.CUR_SCHEMA)


    def add_entry(self, actor: str, category: str, text: str, json: dict):
        with self.Session.begin() as db:
            # Using add for performance, we never update/merge
            # old log entries
            db.add(self.LogEntryRecord(
                created = dt.utcnow(),
                id = str(uuid4()),
                actor = actor,
                source = self.source,
                category = category,
                text = text,
                json = json
            ))
