"""
Alembic environment for the *core* orcha tables.

The database connection is resolved, in priority order, from:

1. ``-x db_url=...`` passed on the alembic command line.
2. The ``sqlalchemy.url`` key in ``alembic.ini`` (if set to a non-empty value).
3. The standard orcha environment variables (the same ones used by the rest of
   the deployment): ``ORCHA_CORE_USER``, ``ORCHA_CORE_PASSWORD``,
   ``ORCHA_CORE_SERVER`` and ``ORCHA_CORE_DB``.

Only the orcha-owned schemas (see ``tables.MANAGED_SCHEMAS``) are considered, so
running this against a database that also contains module/user tables will never
try to drop or alter anything outside of orcha's own schemas.
"""

from __future__ import annotations

import os
from urllib.parse import quote_plus

from alembic import context
from sqlalchemy import create_engine, pool

from orcha.core import tables

# Alembic Config object, provides access to values in alembic.ini.
config = context.config

target_metadata = tables.metadata


def _build_url() -> str:
    """Resolve the database URL from the command line, ini file or env vars."""
    x_args = context.get_x_argument(as_dictionary=True)
    if x_args.get('db_url'):
        return x_args['db_url']

    ini_url = config.get_main_option('sqlalchemy.url')
    if ini_url:
        return ini_url

    user = os.environ['ORCHA_CORE_USER']
    passwd = os.environ['ORCHA_CORE_PASSWORD']
    server = os.environ['ORCHA_CORE_SERVER']
    db = os.environ['ORCHA_CORE_DB']
    return (
        f'postgresql+psycopg://{quote_plus(user)}:{quote_plus(passwd)}'
        f'@{server}/{db}?application_name=orcha_migrations'
    )


def _include_name(name, type_, parent_names):
    """Restrict autogenerate to orcha-owned schemas only."""
    if type_ == 'schema':
        return name in tables.MANAGED_SCHEMAS
    return True


# Shared configuration for both online and offline runs.
_common_opts = dict(
    target_metadata=target_metadata,
    include_schemas=True,
    include_name=_include_name,
    # The single alembic version table lives in the orcha schema and tracks the
    # migration history for every orcha-owned schema.
    version_table='alembic_version',
    version_table_schema=tables.ORCHA_SCHEMA,
    compare_type=True,
)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL to stdout, no DB connection)."""
    context.configure(
        url=_build_url(),
        literal_binds=True,
        dialect_opts={'paramstyle': 'named'},
        **_common_opts,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode against a live database connection."""
    engine = create_engine(_build_url(), poolclass=pool.NullPool)
    with engine.connect() as connection:
        # Make sure every orcha-owned schema exists before we try to create the
        # version table or any objects within them.
        from sqlalchemy.schema import CreateSchema

        for schema in tables.MANAGED_SCHEMAS:
            connection.execute(CreateSchema(schema, if_not_exists=True))
        connection.commit()

        context.configure(connection=connection, **_common_opts)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
