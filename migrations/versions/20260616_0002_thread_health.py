"""thread health table

Adds the ``orcha.thread_health`` table used by ``orcha.core.thread_monitor`` to
persist a snapshot of every supervised background thread (scheduler loops, task
runner handlers, the supervisor itself) so that other processes - notably the
orcha UI - can observe their live state.

One row per (process instance, thread). Each process's ThreadSupervisor replaces
its own rows every cycle and prunes rows left behind by long-gone instances.

Created with ``if_not_exists=True`` so it is safe to apply to a database that
already has the table.

Revision ID: 0002_thread_health
Revises: 0001_initial
Create Date: 2026-06-16 18:40:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '0002_thread_health'
down_revision: Union[str, None] = '0001_initial'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('thread_health',
    sa.Column('instance_id', sa.String(), nullable=False),
    sa.Column('thread_name', sa.String(), nullable=False),
    sa.Column('thread_group', sa.String(), nullable=True),
    sa.Column('state', sa.String(), nullable=True),
    sa.Column('interval_s', sa.Float(), nullable=True),
    sa.Column('heartbeat_timeout_s', sa.Float(), nullable=True),
    sa.Column('started_at', sa.DateTime(), nullable=True),
    sa.Column('last_heartbeat', sa.DateTime(), nullable=True),
    sa.Column('last_tick_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('restart_count', sa.Integer(), nullable=True),
    sa.Column('error_count', sa.Integer(), nullable=True),
    sa.Column('consecutive_errors', sa.Integer(), nullable=True),
    sa.Column('last_error', sa.String(), nullable=True),
    sa.Column('last_error_at', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('instance_id', 'thread_name'),
    schema='orcha',
    if_not_exists=True
    )


def downgrade() -> None:
    op.drop_table('thread_health', schema='orcha', if_exists=True)
