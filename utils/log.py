from __future__ import annotations

from datetime import datetime as dt, timedelta as td
from uuid import uuid4

from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.postgresql import JSON as SQL_JSON
from sqlalchemy.dialects.postgresql import UUID as SQL_UUID
from orcha.utils.sqlalchemy import sqlalchemy_build, postgres_scaffold


class LogManager:

    @staticmethod
    def _setup_sqlalchemy(
            user: str, passwd: str,
            server: str, db: str
        ):
        """
        Setup the SQLAlchemy ORM for the log manager. This function should be
        called before any LogManager instances are created. The schema is
        automatically set to 'orcha_logs'.
        ### Parameters:
        - `user`: The database username.
        - `passwd`: The database password.
        - `server`: The database server.
        - `db`: The database name.
        ### Returns:
        Nothing
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


    def __init__(self, source_name: str):
        """
        Create a new LogManager instance with a given source name.
        Logs are tagged with:
        - Source: All entries created by this instance will have this source.
        - Actor: The actor that performed the action (e.g. a user or a bot).
        - Category: The category of the log entry (e.g. 'error', 'info', 'warning').
        ### Parameters:
        - `source_name`: The name of the source of the logs.
        ### Returns:
        A new LogManager instance.
        """
        self.source = source_name


    def add_entry(self, actor: str, category: str, text: str, json: dict):
        """
        Add a new log entry to the database.
        ### Parameters:
        - `actor`: The actor that performed the action.
        - `category`: The category of the log entry.
        - `text`: The text of the log entry.
        - `json`: A JSON object containing additional information.
        ### Returns:
        Nothing
        """
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

    def prune(self, max_age: td | None = None):
        """
        Prune the logs in the database. Removes no logs if max_age is None.
        ### Parameters:
        - `max_age`: The maximum age of logs to keep. If None, no logs are removed.
        ### Returns:
        The number of logs removed.
        """
        if max_age is None:
            return 0
        with Session.begin() as db:
            return db.query(LogEntryRecord).filter(
                LogEntryRecord.created < dt.utcnow() - max_age
            ).delete()
