"""Alembic migrations for the core orcha tables.

The migrations are shipped as part of orcha core, so they are always in sync with
the table definitions the running code expects. :func:`upgrade_to_head` lets the
application apply them programmatically (e.g. from ``orcha.core.initialise``)
without shelling out to the ``alembic`` CLI.
"""

from __future__ import annotations

import os
from urllib.parse import quote_plus

#: Absolute path to this package, used as the Alembic ``script_location`` so the
#: migrations can be found regardless of the current working directory.
MIGRATIONS_DIR = os.path.dirname(os.path.abspath(__file__))


def build_url(
        user: str, passwd: str, server: str, db: str,
        application_name: str = 'orcha_migrations',
    ) -> str:
    """Build the postgres connection URL used to run migrations.

    Matches the driver/format used by ``orcha.utils.sqlalchemy.postgres_scaffold``
    so migrations connect the same way the rest of orcha does.
    """
    return (
        f'postgresql+psycopg://{quote_plus(user)}:{quote_plus(passwd)}'
        f'@{server}/{db}?application_name={application_name}'
    )


def get_config(url: str):
    """Return an Alembic ``Config`` pointed at the packaged migrations."""
    from alembic.config import Config

    config = Config()
    config.set_main_option('script_location', MIGRATIONS_DIR)
    # env.py reads this as the connection URL (priority below an explicit
    # ``-x db_url`` but above the ORCHA_CORE_* environment variables).
    config.set_main_option('sqlalchemy.url', url)
    return config


def upgrade_to_head(
        user: str, passwd: str, server: str, db: str,
        application_name: str = 'orcha_migrations',
    ) -> None:
    """Apply all outstanding core migrations, bringing the database to ``head``.

    Safe to call on every startup: Alembic only applies revisions that have not
    already been recorded in the ``orcha.alembic_version`` table, so an
    up-to-date database is a no-op.
    """
    from alembic import command

    url = build_url(user, passwd, server, db, application_name)
    command.upgrade(get_config(url), 'head')
