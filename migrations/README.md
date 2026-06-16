# Core orcha database migrations (Alembic)

These migrations manage the **core orcha tables** — the tables orcha itself owns
and that exist for every deployment. They do **not** manage the tables orcha
creates/manages on behalf of user modules (sources, sinks, entities, etc.),
which are still built dynamically at runtime.

Tables tracked here (and the ORM record class that maps onto each one):

| Schema          | Table        | Runtime ORM class                       |
| --------------- | ------------ | --------------------------------------- |
| `orcha`         | `tasks`      | `orcha.core.tasks.TaskRecord`           |
| `orcha`         | `runs`       | `orcha.core.tasks.RunRecord`            |
| `orcha`         | `schedulers` | `orcha.core.scheduler.SchedulerRecord`  |
| `orcha`         | `kvdb_items` | `orcha.utils.kvdb.KvdbItemModel`        |
| `orcha_logs`    | `logs`       | `orcha.utils.log.LogEntryRecord`        |
| `message_queue` | `messages`   | `orcha.utils.mqueue.MessageRecord`      |
| `message_queue` | `consumers`  | `orcha.utils.mqueue.ConsumerRecord`     |

The table definitions are the **single source of truth** in
[`orcha/core/tables.py`](../core/tables.py): both the runtime ORM classes
(via ``__table__``) and Alembic's autogeneration target read from it, so there is
nothing to keep in sync. To change a core table, edit `orcha/core/tables.py` and
generate a migration (see below). A single `alembic_version` table in the `orcha`
schema tracks history across all of these schemas.

## Configuration

[`alembic.ini`](../../alembic.ini) lives at the repository root; run `alembic`
from there. The database connection is resolved in
[`env.py`](env.py), in priority order:

1. `-x db_url=...` on the command line.
2. `sqlalchemy.url` in `alembic.ini` (empty by default).
3. The standard orcha env vars: `ORCHA_CORE_USER`, `ORCHA_CORE_PASSWORD`,
   `ORCHA_CORE_SERVER` (host:port), `ORCHA_CORE_DB`.

Only the orcha-owned schemas are considered, so running against a database that
also holds module/user tables will never touch anything outside `orcha` /
`orcha_logs`.

## Common commands

```bash
# Apply all migrations to a fresh/empty database
alembic upgrade head

# Mark an EXISTING database (tables already built by create_all) as up to date
# without re-creating anything:
alembic stamp 0001_initial

# Show the current revision / pending changes
alembic current
alembic check

# Generate a new migration after editing orcha/core/tables.py
alembic revision --autogenerate -m "describe the change"

# Emit SQL instead of applying it (offline mode)
alembic upgrade head --sql

# Override the connection for a one-off command
alembic -x db_url=postgresql+psycopg://user:pass@host:5432/db upgrade head
```

## Existing deployments

The initial migration (`0001_initial`) reproduces the schema that historically
was built by `sqlalchemy_build` / `create_all`. For a database that already has
these tables, run `alembic stamp 0001_initial` (not `upgrade`) so future
migrations apply cleanly. For a brand new database, `alembic upgrade head`
creates the schemas and tables from scratch.
