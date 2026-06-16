"""
Shared database wiring for orcha core.

This module owns the two things every core module used to set up for itself:

* ``Base`` — a single declarative registry bound to ``orcha.core.tables.metadata``
  that all of the core ORM record classes map onto (via ``__table__``). The
  record classes can therefore be defined once at module import time instead of
  being rebuilt on every ``initialise()`` call.
* ``session_maker`` — a single, process-wide ``sessionmaker`` shared by the core
  modules (tasks, scheduler, logging). It is created unbound at import and bound
  to the engine by :func:`configure`, which ``orcha.core.initialise`` calls once.

Components that legitimately talk to a *different* database keep their own
session factory rather than using this one: the kvdb store (which accepts its own
connection parameters) and the message queue broker (a separate service).
"""
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, registry, sessionmaker

from orcha.core import tables
from orcha.utils.sqlalchemy import postgres_partial_scaffold

# One registry/declarative base for all core ORM record classes. Its metadata is
# the single source of truth in orcha.core.tables, so classes that map onto those
# tables share exactly the schema the migrations create.
mapper_registry = registry(metadata=tables.metadata)
Base = mapper_registry.generate_base()

# The shared engine and session factory. ``session_maker`` is usable as soon as
# this module is imported, but only actually connects once ``configure`` has
# bound it to an engine (done by orcha.core.initialise).
engine: Engine | None = None
session_maker: sessionmaker[Session] = sessionmaker(expire_on_commit=False)


def configure(
        user: str, passwd: str, server: str, db: str,
        application_name: str = 'orcha',
    ) -> None:
    """Create the shared engine and bind the shared ``session_maker`` to it.

    Called once by ``orcha.core.initialise``. Safe to call again to re-point the
    shared connection (e.g. in tests); it simply rebinds the session factory.
    """
    global engine
    engine, _ = postgres_partial_scaffold(user, passwd, server, db, application_name)
    session_maker.configure(bind=engine)


def is_configured() -> bool:
    """Return True once :func:`configure` has bound the shared engine."""
    return engine is not None
