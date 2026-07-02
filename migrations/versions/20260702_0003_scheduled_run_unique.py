"""unique scheduled run slot

Adds a partial unique index on ``orcha.runs`` so that at most one *scheduled* run
can exist per ``(task_idf, set_idf, scheduled_time)``. This lets multiple
schedulers run safely: if two schedulers both decide the same slot is due, the
second INSERT collides on this index and is handled as "already produced" rather
than double-producing the run.

The index is partial (``WHERE run_type = 'scheduled'``) so manual, triggered and
retry runs -- which legitimately have no single-slot uniqueness -- are unaffected.

Created with ``if_not_exists=True`` so it is safe to re-apply. Note: applying this
to a database that already contains duplicate scheduled runs for the same slot
will fail; such duplicates must be resolved first (they should not occur under
single-scheduler operation).

Revision ID: 0003_scheduled_run_unique
Revises: 0002_thread_health
Create Date: 2026-07-02 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '0003_scheduled_run_unique'
down_revision: Union[str, None] = '0002_thread_health'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        'uq_orcha_runs_scheduled_slot',
        'runs',
        ['task_idf', 'set_idf', 'scheduled_time'],
        unique=True,
        schema='orcha',
        postgresql_where=sa.text("run_type = 'scheduled'"),
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_index(
        'uq_orcha_runs_scheduled_slot',
        table_name='runs',
        schema='orcha',
        if_exists=True,
    )
