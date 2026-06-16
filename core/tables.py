"""
Canonical SQLAlchemy table definitions for the *core* orcha tables.

This module is the **single source of truth** for the schema of the tables orcha
itself owns and that exist for every deployment: the orchestration tables
(tasks, runs, schedulers, the kvdb store), the logs table and the message queue
tables. It is intentionally separate from the tables orcha *creates and manages*
on behalf of user modules (sources, sinks, entities, etc.), which are defined
dynamically at runtime.

Two consumers share these definitions:

* The runtime ORM record classes map onto these ``Table`` objects via
  ``__table__`` (e.g. ``orcha.core.tasks.TaskRecord``,
  ``orcha.utils.log.LogEntryRecord``), so the application queries the
  exact tables defined here.
* Alembic targets ``metadata`` as its ``target_metadata`` for autogeneration
  (see ``orcha.migrations.env``).

Change a column here and both the running code and the migration autogeneration
see it; there is nowhere else to keep in sync.
"""

from sqlalchemy import (
    Column,
    DateTime,
    Index,
    LargeBinary,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import JSON as PG_JSON
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

# The schema that holds the orchestration tables (tasks/runs/schedulers/kvdb).
# Mirrors ``orcha.core._ORCHA_SCHEMA``.
ORCHA_SCHEMA = 'orcha'

# The schema that holds the logging table (used by ``orcha.utils.log``).
ORCHA_LOGS_SCHEMA = 'orcha_logs'

# The schema that holds the message queue tables. Mirrors the default
# ``mqueue_pg_schema`` in ``orcha.utils.mqueue.Broker.setup``.
MESSAGE_QUEUE_SCHEMA = 'message_queue'

# A single MetaData holding tables across all orcha-owned schemas. Alembic uses
# this as ``target_metadata`` for autogeneration with ``include_schemas=True``.
metadata = MetaData()


# ---------------------------------------------------------------------------
# orcha schema
# ---------------------------------------------------------------------------

tasks = Table(
    'tasks',
    metadata,
    Column('task_idk', String, primary_key=True),
    Column('version', DateTime(timezone=False), primary_key=True),
    Column('task_metadata', PG_JSON),
    Column('task_tags', PG_JSON),
    Column('name', String),
    Column('description', String),
    Column('schedule_sets', PG_JSON),
    Column('thread_group', String),
    Column('last_active', DateTime(timezone=False)),
    Column('status', String),
    Column('notes', String),
    Column('task_config', PG_JSON),
    schema=ORCHA_SCHEMA,
)

runs = Table(
    'runs',
    metadata,
    Column('update_timestamp', DateTime(timezone=False)),
    Column('run_idk', String, primary_key=True),
    Column('task_idf', String),
    Column('set_idf', String),
    Column('run_type', String),
    Column('created_time', DateTime(timezone=False)),
    Column('created_by', String),
    Column('scheduled_time', DateTime(timezone=False)),
    Column('start_time', DateTime(timezone=False)),
    Column('end_time', DateTime(timezone=False)),
    Column('last_active', DateTime(timezone=False)),
    Column('config', PG_JSON),
    Column('progress', String),
    Column('status', String),
    Column('output', PG_JSON),
    Index('idx_orcha_runs_task_scheduled', 'task_idf', 'scheduled_time', 'run_type'),
    Index(
        'idx_orcha_runs_task_set_scheduled',
        'task_idf', 'scheduled_time', 'set_idf', 'run_type',
    ),
    Index('idx_orcha_runs_taskidf_status_progress', 'task_idf', 'status', 'progress'),
    schema=ORCHA_SCHEMA,
)

schedulers = Table(
    'schedulers',
    metadata,
    Column('scheduler_idk', String, primary_key=True),
    Column('last_active', DateTime(timezone=False)),
    Column('loaded_at', DateTime(timezone=False)),
    schema=ORCHA_SCHEMA,
)

kvdb_items = Table(
    'kvdb_items',
    metadata,
    Column('key', String, primary_key=True),
    Column('value', LargeBinary, nullable=False),
    Column('type', String, nullable=False),
    Column('expiry', DateTime),
    Column('salt', LargeBinary, nullable=True),
    schema=ORCHA_SCHEMA,
)


# ---------------------------------------------------------------------------
# orcha_logs schema
# ---------------------------------------------------------------------------

logs = Table(
    'logs',
    metadata,
    Column('created', DateTime, index=True),
    Column('id', PG_UUID(as_uuid=True), primary_key=True),
    Column('actor', String),
    Column('source', String, index=True),
    Column('category', String),
    Column('text', String),
    Column('json', PG_JSON),
    schema=ORCHA_LOGS_SCHEMA,
)


# ---------------------------------------------------------------------------
# message_queue schema
# ---------------------------------------------------------------------------

messages = Table(
    'messages',
    metadata,
    Column('id', String, primary_key=True),
    Column('created_at', DateTime),
    Column('sent_at', DateTime),
    Column('acked_at', DateTime),
    Column('channel', String),
    Column('consumer_name', String),
    Column('message', String),
    Column('acked', String),
    Column('send_status', String),
    schema=MESSAGE_QUEUE_SCHEMA,
)

consumers = Table(
    'consumers',
    metadata,
    Column('channel', String, primary_key=True),
    Column('name', String, primary_key=True),
    Column('url', String),
    schema=MESSAGE_QUEUE_SCHEMA,
)


#: Every schema owned by orcha core. Used by the Alembic env to restrict
#: autogeneration to orcha-owned objects and to ensure the schemas exist.
MANAGED_SCHEMAS = (ORCHA_SCHEMA, ORCHA_LOGS_SCHEMA, MESSAGE_QUEUE_SCHEMA)
