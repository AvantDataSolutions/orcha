"""initial core tables

This is the initial migration for the core orcha tables. It represents the
schema as it already exists in deployed databases (the tables built historically
by ``sqlalchemy_build`` / ``create_all``): the orchestration tables in the
``orcha`` schema (tasks, runs, schedulers, kvdb_items), the logging table in the
``orcha_logs`` schema and the message queue tables in the ``message_queue``
schema.

Every object is created with ``IF NOT EXISTS`` so this migration is safe to run
against a database that already contains some or all of these tables (e.g. an
existing deployment that was never stamped). Already-present objects are skipped,
missing ones are created, which lets a database that is behind catch up cleanly.


Revision ID: 0001_initial
Revises:
Create Date: 2026-06-16 16:21:13.717799

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0001_initial'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Ensure the orcha-owned schemas exist. In online mode env.py also creates
    # these (the version table lives in ``orcha`` and must exist first), but
    # doing it here keeps the migration self-contained for offline/``--sql`` use.
    op.execute('CREATE SCHEMA IF NOT EXISTS orcha')
    op.execute('CREATE SCHEMA IF NOT EXISTS orcha_logs')
    op.execute('CREATE SCHEMA IF NOT EXISTS message_queue')

    # Every create below uses ``if_not_exists=True`` so this initial migration is
    # safe to apply to a database that already contains some (or all) of these
    # objects. Already-present objects are skipped; missing ones are created.
    op.create_table('kvdb_items',
    sa.Column('key', sa.String(), nullable=False),
    sa.Column('value', sa.LargeBinary(), nullable=False),
    sa.Column('type', sa.String(), nullable=False),
    sa.Column('expiry', sa.DateTime(), nullable=True),
    sa.Column('salt', sa.LargeBinary(), nullable=True),
    sa.PrimaryKeyConstraint('key'),
    schema='orcha',
    if_not_exists=True
    )
    op.create_table('runs',
    sa.Column('update_timestamp', sa.DateTime(), nullable=True),
    sa.Column('run_idk', sa.String(), nullable=False),
    sa.Column('task_idf', sa.String(), nullable=True),
    sa.Column('set_idf', sa.String(), nullable=True),
    sa.Column('run_type', sa.String(), nullable=True),
    sa.Column('created_time', sa.DateTime(), nullable=True),
    sa.Column('created_by', sa.String(), nullable=True),
    sa.Column('scheduled_time', sa.DateTime(), nullable=True),
    sa.Column('start_time', sa.DateTime(), nullable=True),
    sa.Column('end_time', sa.DateTime(), nullable=True),
    sa.Column('last_active', sa.DateTime(), nullable=True),
    sa.Column('config', postgresql.JSON(astext_type=sa.Text()), nullable=True),
    sa.Column('progress', sa.String(), nullable=True),
    sa.Column('status', sa.String(), nullable=True),
    sa.Column('output', postgresql.JSON(astext_type=sa.Text()), nullable=True),
    sa.PrimaryKeyConstraint('run_idk'),
    schema='orcha',
    if_not_exists=True
    )
    op.create_index('idx_orcha_runs_task_scheduled', 'runs', ['task_idf', 'scheduled_time', 'run_type'], unique=False, schema='orcha', if_not_exists=True)
    op.create_index('idx_orcha_runs_task_set_scheduled', 'runs', ['task_idf', 'scheduled_time', 'set_idf', 'run_type'], unique=False, schema='orcha', if_not_exists=True)
    op.create_index('idx_orcha_runs_taskidf_status_progress', 'runs', ['task_idf', 'status', 'progress'], unique=False, schema='orcha', if_not_exists=True)
    op.create_table('schedulers',
    sa.Column('scheduler_idk', sa.String(), nullable=False),
    sa.Column('last_active', sa.DateTime(), nullable=True),
    sa.Column('loaded_at', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('scheduler_idk'),
    schema='orcha',
    if_not_exists=True
    )
    op.create_table('tasks',
    sa.Column('task_idk', sa.String(), nullable=False),
    sa.Column('version', sa.DateTime(), nullable=False),
    sa.Column('task_metadata', postgresql.JSON(astext_type=sa.Text()), nullable=True),
    sa.Column('task_tags', postgresql.JSON(astext_type=sa.Text()), nullable=True),
    sa.Column('name', sa.String(), nullable=True),
    sa.Column('description', sa.String(), nullable=True),
    sa.Column('schedule_sets', postgresql.JSON(astext_type=sa.Text()), nullable=True),
    sa.Column('thread_group', sa.String(), nullable=True),
    sa.Column('last_active', sa.DateTime(), nullable=True),
    sa.Column('status', sa.String(), nullable=True),
    sa.Column('notes', sa.String(), nullable=True),
    sa.Column('task_config', postgresql.JSON(astext_type=sa.Text()), nullable=True),
    sa.PrimaryKeyConstraint('task_idk', 'version'),
    schema='orcha',
    if_not_exists=True
    )
    op.create_table('logs',
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('actor', sa.String(), nullable=True),
    sa.Column('source', sa.String(), nullable=True),
    sa.Column('category', sa.String(), nullable=True),
    sa.Column('text', sa.String(), nullable=True),
    sa.Column('json', postgresql.JSON(astext_type=sa.Text()), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='orcha_logs',
    if_not_exists=True
    )
    op.create_index(op.f('ix_orcha_logs_logs_created'), 'logs', ['created'], unique=False, schema='orcha_logs', if_not_exists=True)
    op.create_index(op.f('ix_orcha_logs_logs_source'), 'logs', ['source'], unique=False, schema='orcha_logs', if_not_exists=True)
    op.create_table('consumers',
    sa.Column('channel', sa.String(), nullable=False),
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('url', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('channel', 'name'),
    schema='message_queue',
    if_not_exists=True
    )
    op.create_table('messages',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('sent_at', sa.DateTime(), nullable=True),
    sa.Column('acked_at', sa.DateTime(), nullable=True),
    sa.Column('channel', sa.String(), nullable=True),
    sa.Column('consumer_name', sa.String(), nullable=True),
    sa.Column('message', sa.String(), nullable=True),
    sa.Column('acked', sa.String(), nullable=True),
    sa.Column('send_status', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='message_queue',
    if_not_exists=True
    )


def downgrade() -> None:
    # ``if_exists=True`` so a partial/repeated downgrade does not error.
    op.drop_table('messages', schema='message_queue', if_exists=True)
    op.drop_table('consumers', schema='message_queue', if_exists=True)
    op.drop_index(op.f('ix_orcha_logs_logs_source'), table_name='logs', schema='orcha_logs', if_exists=True)
    op.drop_index(op.f('ix_orcha_logs_logs_created'), table_name='logs', schema='orcha_logs', if_exists=True)
    op.drop_table('logs', schema='orcha_logs', if_exists=True)
    op.drop_table('tasks', schema='orcha', if_exists=True)
    op.drop_table('schedulers', schema='orcha', if_exists=True)
    op.drop_index('idx_orcha_runs_taskidf_status_progress', table_name='runs', schema='orcha', if_exists=True)
    op.drop_index('idx_orcha_runs_task_set_scheduled', table_name='runs', schema='orcha', if_exists=True)
    op.drop_index('idx_orcha_runs_task_scheduled', table_name='runs', schema='orcha', if_exists=True)
    op.drop_table('runs', schema='orcha', if_exists=True)
    op.drop_table('kvdb_items', schema='orcha', if_exists=True)
