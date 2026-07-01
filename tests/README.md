# Orcha core tests

Pytest-based tests for `orcha` core (tasks, runs, scheduler, task runner).

## Layout

```
orcha/tests/
  conftest.py            # sys.path setup, clock fixture, DB-env skip guard
  helpers.py             # env vars, empty_database(), FakeClock, noop_task
  pytest.ini             # config (defaults to the core suite)
  core/                  # tests that run against an INITIALISED orcha
    conftest.py          # session initialise(), per-test DB truncate,
                         #   scheduler + make_task fixtures, Producer patch
    test_tasks.py
    test_runs.py
    test_scheduler.py    # deterministic, fake-clock, no sleeps
    test_runner.py
  uninitialised/         # tests that orcha refuses to work before initialise()
    test_uninitialised.py
  db/                    # backend-specific helpers in orcha.utils.sqlalchemy
    conftest.py          # per-backend engine/session fixtures (self-gating)
    test_mssql.py        # mssql_upsert
    test_postgres.py     # postgres_upsert, get(), get_latest_versions()
    test_sqlite.py       # sqlite_upsert, sqlalchemy_replace
```

## Key ideas

- **Deterministic time.** Tests pin orcha's clock via `orcha.set_time` (the
  `clock` fixture / `FakeClock`) and drive the scheduler by calling its
  `_tick_*` methods directly, then advance the clock across cron boundaries.
  No `time.sleep`, no waiting on background threads.
- **Per-test isolation via truncate.** Every core test starts from a truncated
  database (`clean_db`, autouse). Truncate — not transactional rollback — is
  used deliberately: the scheduler/runner open their own DB connections, so a
  rollback on the test connection would not undo their writes.
- **No mqueue broker needed.** `Producer.send_message` is patched to record
  emitted messages, so tests can assert on alerts (inactive task, historical
  run, run failed) without a running broker.
- **Task functions.** Defined at module level here, mirroring real apps. (Orcha
  dedents source before parsing config keys, so indented/nested functions also
  work.)

## Running

The two suites must run as **separate processes**: once `initialise()` has run
it cannot be undone, so the uninitialised checks only hold in a fresh process.

### With docker compose (self-contained)

```bash
cd orcha/tests
docker compose run --rm orcha-tests                 # core suite
docker compose run --rm orcha-tests-uninitialised   # uninitialised suite
docker compose down -v
```

### Locally against a Postgres

Point the suite at any empty Postgres (it runs its own migrations). For example
using the compose database exposed on `localhost:6432`:

```bash
cd orcha/tests && docker compose up -d orcha-tests-db && cd ../..

export ORCHA_CORE_USER=orcha_user
export ORCHA_CORE_PASSWORD=orcha_pass
export ORCHA_CORE_SERVER=localhost:6432
export ORCHA_CORE_DB=orcha

pytest orcha/tests/core
pytest orcha/tests/uninitialised
```

If the `ORCHA_CORE_*` environment variables are not set, the DB-backed tests are
skipped with a clear message rather than failing.

## Database suite (`db`)

Tests for the backend-specific helpers in `orcha.utils.sqlalchemy`
(`mssql_upsert`, `postgres_upsert`, `sqlite_upsert`, `sqlalchemy_replace`,
`get`, `get_latest_versions`), run against real engines. It does **not** require
`orcha` to be initialised, and each backend gates itself:

- **sqlite** always runs (an on-disk temp database, no service needed).
- **mssql** runs when `ORCHA_MSSQL_*` are set, otherwise skips.
- **postgres** runs when `ORCHA_CORE_*` are set (reuses the shared test
  Postgres), otherwise skips.

It is not part of the default `testpaths`, so run it explicitly:

```bash
cd orcha/tests
docker compose up -d orcha-tests-mssql orcha-tests-db   # SQL Server + Postgres
cd ../..

# SQL Server (exposed on localhost:14330 by the compose override)
export ORCHA_MSSQL_USER=sa
export ORCHA_MSSQL_PASSWORD=Orcha_test_pass1
export ORCHA_MSSQL_SERVER=localhost:14330
export ORCHA_MSSQL_DB=orcha_test
# Postgres (as above): ORCHA_CORE_USER/PASSWORD/SERVER/DB

pytest orcha/tests/db
```

The SQL Server container (`mcr.microsoft.com/mssql/server`) needs ~2 GB RAM. The
target database is created automatically from `master` on first run. With no env
vars set, only the sqlite tests run and the rest skip.

> One test — `test_all_columns_are_primary_key` — is an `xfail`: it documents a
> known `mssql_upsert` bug (an all-primary-key table produces an empty
> `MERGE ... UPDATE SET`, a syntax error). When the pending `mssql_upsert`
> optimisation fixes it, the strict xfail will flip to a failure — remove the
> marker at that point.
