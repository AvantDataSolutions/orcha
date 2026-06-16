from __future__ import annotations

from datetime import datetime as dt, timedelta as td
from uuid import uuid4

from orcha.core import tables
from orcha.core.database import Base, session_maker


# ORM record class mapped onto the single-source-of-truth table in
# orcha.core.tables; the shared engine/session_maker live in orcha.core.database.
# The orcha_logs schema and the logs table are created and owned by the Alembic
# migrations in orcha.migrations (run `alembic upgrade head`); not built here.
class LogEntryRecord(Base):
    __table__ = tables.logs


class LogManager:
    """
    The base class for logging into a database.
    This is designed for very simple logging.
    Also provides helpers for querying logs.
    """

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
        with session_maker.begin() as db:
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
        with session_maker.begin() as db:
            return db.query(LogEntryRecord).filter(
                LogEntryRecord.created < dt.utcnow() - max_age
            ).delete()

    @staticmethod
    def get_entries(
        limit: int | None = None,
        sources: list[str] | None = None,
        start: dt | None = None,
        end: dt | None = None
    ):
        """
        Get log entries from the database, optionally filtered by sources and date range.
        ### Parameters:
        - `limit`: The maximum number of log entries to return. If None, return all entries.
        - `sources`: The sources of the log entries to return. If None, return entries from all sources.
        - `start`: Only include entries created >= start (if provided)
        - `end`: Only include entries created <= end (if provided)
        ### Returns:
        A list of log entries.
        """
        with session_maker.begin() as db:
            query = db.query(LogEntryRecord)
            if start is not None:
                query = query.filter(LogEntryRecord.created >= start)
            if end is not None:
                query = query.filter(LogEntryRecord.created <= end)
            if sources is not None and len(sources) > 0:
                sources_formatted = [
                    s.lower().strip().replace(" ", "_")
                    for s in sources if s and len(s) > 0
                ]
                query = query.filter(LogEntryRecord.source.in_(sources_formatted))
            query = query.order_by(LogEntryRecord.created.desc())
            if limit is not None and limit > 0:
                query = query.limit(limit)
            return query.all()

    @staticmethod
    def get_distinct_sources() -> list[str]:
        """Return a sorted list of distinct log sources."""
        with session_maker.begin() as db:
            rows = db.query(LogEntryRecord.source).distinct().all()
            sources: list[str] = [r[0] for r in rows if r and r[0]]
        return sorted(sources)
