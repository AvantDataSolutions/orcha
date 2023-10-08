from __future__ import annotations
from uuid import uuid4

from datetime import datetime as dt

from sqlalchemy import Column, MetaData, String, create_engine, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID as SQL_UUID, JSON as SQL_JSON
from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import sessionmaker

from utils.sqlalchemy import postgres_build, postgres_scaffold

from orcha.core.credentials import *


class LogManager:

    CUR_SCHEMA = 'logs'
    Base, engine, Session = postgres_scaffold(CUR_SCHEMA)


    class LogEntryRecord(Base):
        __tablename__ = 'entries'
        entry_created = Column(DateTime)
        entry_id = Column(SQL_UUID(as_uuid=True), primary_key=True)
        entry_actor = Column(String)
        entry_source = Column(String)
        entry_category = Column(String)
        entry_text = Column(String)
        entry_json = Column(SQL_JSON)


    def __init__(self, source_name: str) -> None:
        self.source = source_name
        postgres_build(self.Base, self.engine, self.CUR_SCHEMA)


    def add_entry(self, category: str, text: str, json: dict):
        with self.Session.begin() as db:
            # Using add for performance, we never update/merge
            # old log entries
            db.add(self.LogEntryRecord(
                entry_created = dt.utcnow(),
                entry_id = uuid4().hex,
                entry_actor = None,
                entry_source = self.source,
                entry_category = category,
                entry_text = text,
                entry_json = json
            ))

