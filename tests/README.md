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
