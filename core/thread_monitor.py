"""
Generalised thread lifecycle monitoring for orcha.

This module provides a small, dependency-light framework for running the
long-lived background loops that orcha relies on (the scheduler maintenance
loops and the task runner handler loops) in a way that is *supervised*,
*self-healing* and *observable*.

The three pieces are:

- ``ManagedThread``: wraps a periodic "tick" function in a loop that
  - catches exceptions from the tick so a transient error (e.g. a momentary
    database blip) no longer silently kills the thread - it simply records the
    error and retries on the next interval,
  - records a heartbeat so stalls (a tick that hangs) can be detected,
  - sleeps in small interruptible chunks so ``stop()`` is responsive,
  - tracks lifecycle metadata (state, restart/error counts, last error).

- ``ThreadRegistry``: a process-global registry of every ``ManagedThread`` so
  the whole process's thread health can be enumerated in one place.

- ``ThreadSupervisor``: a single watchdog thread that restarts any managed
  thread that has genuinely died (belt-and-braces on top of the per-tick
  exception handling) and periodically writes a snapshot of all thread health
  to the database so that a *different* process (notably the orcha UI) can see
  the live state of the scheduler/runner threads.

The DB persistence is optional: until ``orcha.core.initialise`` has configured
the shared connection (``orcha.core.database``), the supervisor still runs and
still restarts dead threads, it just doesn't persist snapshots. This keeps the
framework usable in tests and lightweight setups. The ``thread_health`` table is
defined in ``orcha.core.tables`` and created by the orcha migrations.
"""
from __future__ import annotations

import os
import socket
import threading
from dataclasses import dataclass
from datetime import datetime as dt
from datetime import timedelta as td
from enum import Enum

from orcha import current_time
from orcha.core import tables
from orcha.core.database import Base, is_configured, session_maker
from orcha.utils.log import LogManager

tm_log = LogManager('thread_monitor')

# Default heartbeat chunk - the longest a managed thread will sleep before
# refreshing its heartbeat while idling between ticks.
_BEAT_CHUNK_SECONDS = 5.0


def _safe_log(category: str, text: str, json: dict | None = None) -> None:
    """
    Log without ever raising. The whole point of this module is to keep threads
    alive, so a logging failure (e.g. the logging DB isn't set up yet, or a
    transient outage) must never propagate up and kill a supervised thread.
    """
    try:
        tm_log.add_entry(
            actor='thread_monitor', category=category, text=text, json=json or {}
        )
    except Exception:
        pass


class ThreadState(str, Enum):
    """
    The lifecycle state of a managed thread.

    - ``starting``: the thread has been created but hasn't completed a tick yet.
    - ``running``: alive and actively running ticks without error.
    - ``idle``: alive but its run condition is currently False (e.g. the
      scheduler is stopped/paused) so it is intentionally not doing work.
    - ``errored``: alive and looping, but the most recent tick raised.
    - ``stalled``: alive but the heartbeat is older than the configured timeout,
      i.e. a tick appears to be hung.
    - ``crashed``: the underlying thread is no longer alive and was not stopped
      intentionally.
    - ``stopped``: intentionally stopped via ``stop()``.
    """
    starting = 'starting'
    running = 'running'
    idle = 'idle'
    errored = 'errored'
    stalled = 'stalled'
    crashed = 'crashed'
    stopped = 'stopped'


# States that indicate something is wrong and should be surfaced/alerted.
UNHEALTHY_STATES = {ThreadState.errored, ThreadState.stalled, ThreadState.crashed}


@dataclass
class ThreadStatus:
    """
    An immutable point-in-time snapshot of a ``ManagedThread``'s health.
    Safe to pass across module boundaries (e.g. to the UI query layer).
    """
    name: str
    group: str
    state: str
    interval_s: float
    heartbeat_timeout_s: float | None
    started_at: dt | None
    last_heartbeat: dt | None
    last_tick_at: dt | None
    restart_count: int
    error_count: int
    consecutive_errors: int
    last_error: str | None
    last_error_at: dt | None


class ManagedThread:
    """
    A supervised, self-healing periodic loop.

    A ``ManagedThread`` owns the boilerplate that every orcha background loop
    used to repeat by hand: the ``while`` loop, the ``sleep``, and (newly) the
    exception handling, heartbeat and lifecycle bookkeeping. Callers provide a
    ``tick`` callable that performs a single iteration of work; the managed
    thread takes care of running it on ``interval`` and keeping going if it
    raises.

    ### Args
    - ``name``: unique (within the process) name for the thread.
    - ``tick``: the callable to run once per interval.
    - ``interval``: seconds to wait between ticks.
    - ``group``: a logical grouping (e.g. ``scheduler``/``task_runner``) used
      for display and filtering.
    - ``startup_delay``: seconds to wait before the first tick.
    - ``run_condition``: optional predicate; when it returns False the tick is
      skipped (the thread reports ``idle``) but the thread stays alive and keeps
      heartbeating. Used to model the scheduler's stopped/paused state.
    - ``heartbeat_timeout``: if set, a heartbeat older than this marks the
      thread ``stalled``. Leave as ``None`` for loops whose ticks may
      legitimately run for a long time (e.g. running an actual task).
    - ``auto_restart``: whether the supervisor should restart this thread if it
      dies. Defaults to True.
    - ``max_consecutive_errors``: if set, the loop exits after this many
      back-to-back tick failures (the supervisor will then restart it, giving a
      clean slate). Defaults to ``None`` (loop forever, self-healing in place).
    - ``ensure_supervisor``: whether starting this thread should also ensure the
      global supervisor is running. Only the supervisor itself sets this False.
    - ``register``: whether to add this thread to the global registry.
    """

    def __init__(
            self,
            name: str,
            tick,
            interval: float,
            *,
            group: str = 'default',
            startup_delay: float = 0.0,
            run_condition=None,
            heartbeat_timeout: float | None = None,
            auto_restart: bool = True,
            max_consecutive_errors: int | None = None,
            ensure_supervisor: bool = True,
            register: bool = True,
        ):
        self.name = name
        self.group = group
        self._tick = tick
        self.interval = interval
        self.startup_delay = startup_delay
        self._run_condition = run_condition
        self.heartbeat_timeout = heartbeat_timeout
        self.auto_restart = auto_restart
        self.max_consecutive_errors = max_consecutive_errors
        self._ensure_supervisor = ensure_supervisor

        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # Lifecycle metadata - guarded by self._lock for cross-thread reads.
        self._base_state = ThreadState.stopped
        self.started_at: dt | None = None
        self.last_heartbeat: dt | None = None
        self.last_tick_at: dt | None = None
        self.last_error: str | None = None
        self.last_error_at: dt | None = None
        self.restart_count = 0
        self.error_count = 0
        self.consecutive_errors = 0

        if register:
            ThreadRegistry.register(self)

    # -- control ---------------------------------------------------------

    def start(self) -> 'ManagedThread':
        """
        Start the loop. Safe to call repeatedly: if the thread is already alive
        this does nothing.
        """
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return self
            self._stop_event.clear()
            self._base_state = ThreadState.starting
            self._thread = threading.Thread(
                target=self._loop, name=self.name, daemon=True
            )
            self._thread.start()
        if self._ensure_supervisor:
            ThreadSupervisor.ensure_running()
        return self

    def restart(self) -> None:
        """
        Restart a thread that has died. Increments the restart counter. Called
        by the supervisor; safe to call manually.
        """
        with self._lock:
            self.restart_count += 1
        _safe_log(
            'restart', f'Restarting thread {self.name}',
            {'name': self.name, 'group': self.group, 'restart_count': self.restart_count}
        )
        self.start()

    def stop(self, join: bool = True, timeout: float | None = None) -> None:
        """
        Signal the loop to stop. With ``join`` the call blocks until the loop
        actually exits (up to ``timeout`` seconds, or indefinitely if None).
        """
        self._stop_event.set()
        thread = self._thread
        if join and thread is not None and thread is not threading.current_thread():
            thread.join(timeout=timeout)

    def beat(self) -> None:
        """
        Record a heartbeat. Cheap and thread-safe so it can be called from
        helper threads doing work on behalf of this loop (e.g. the task runner
        updating active times during a long-running task).
        """
        with self._lock:
            self.last_heartbeat = current_time()

    # -- introspection ---------------------------------------------------

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def is_stopping(self) -> bool:
        return self._stop_event.is_set()

    def _effective_state(self) -> ThreadState:
        """
        Compute the externally-visible state, factoring in liveness and
        heartbeat staleness on top of the loop's own self-reported base state.
        """
        if self._stop_event.is_set() and not self.is_alive():
            return ThreadState.stopped
        if not self.is_alive():
            # Never started yet looks 'stopped'; died unexpectedly is 'crashed'.
            if self.started_at is None:
                return ThreadState.stopped
            return ThreadState.crashed
        if self.heartbeat_timeout is not None and self.last_heartbeat is not None:
            age = (current_time() - self.last_heartbeat).total_seconds()
            if age > self.heartbeat_timeout:
                return ThreadState.stalled
        return self._base_state

    def status(self) -> ThreadStatus:
        with self._lock:
            return ThreadStatus(
                name=self.name,
                group=self.group,
                state=self._effective_state().value,
                interval_s=self.interval,
                heartbeat_timeout_s=self.heartbeat_timeout,
                started_at=self.started_at,
                last_heartbeat=self.last_heartbeat,
                last_tick_at=self.last_tick_at,
                restart_count=self.restart_count,
                error_count=self.error_count,
                consecutive_errors=self.consecutive_errors,
                last_error=self.last_error,
                last_error_at=self.last_error_at,
            )

    # -- loop ------------------------------------------------------------

    def _interruptible_wait(self, seconds: float) -> None:
        """
        Wait up to ``seconds``, returning early if a stop is requested. The wait
        is broken into chunks so the heartbeat stays fresh while idling (so an
        idle loop is never mistaken for a stalled one).
        """
        remaining = seconds
        while remaining > 0 and not self._stop_event.is_set():
            chunk = min(_BEAT_CHUNK_SECONDS, remaining)
            self.beat()
            if self._stop_event.wait(timeout=chunk):
                return
            remaining -= chunk

    def _loop(self) -> None:
        with self._lock:
            self.started_at = current_time()
            self._base_state = ThreadState.starting
        self.beat()

        if self.startup_delay:
            self._interruptible_wait(self.startup_delay)

        while not self._stop_event.is_set():
            self.beat()
            should_run = self._run_condition is None or self._run_condition()
            if should_run:
                try:
                    self._tick()
                    with self._lock:
                        self.consecutive_errors = 0
                        self.last_tick_at = current_time()
                        self._base_state = ThreadState.running
                except Exception as e:
                    with self._lock:
                        self.error_count += 1
                        self.consecutive_errors += 1
                        self.last_error = f'{type(e).__name__}: {e}'
                        self.last_error_at = current_time()
                        self._base_state = ThreadState.errored
                        consecutive = self.consecutive_errors
                        last_error = self.last_error
                    _safe_log(
                        'tick_error', f'Tick error in thread {self.name}',
                        {
                            'name': self.name,
                            'group': self.group,
                            'error': last_error,
                            'consecutive_errors': consecutive,
                        }
                    )
                    if (
                        self.max_consecutive_errors is not None
                        and consecutive >= self.max_consecutive_errors
                    ):
                        # Bail out; the supervisor will restart us cleanly.
                        break
            else:
                with self._lock:
                    self._base_state = ThreadState.idle
            self._interruptible_wait(self.interval)

        with self._lock:
            self._base_state = ThreadState.stopped


class ThreadRegistry:
    """
    Process-global registry of all ``ManagedThread`` instances. Lets the
    supervisor and any introspection (e.g. the UI query layer running in the
    same process) enumerate every supervised thread in one place.
    """
    _threads: dict[str, ManagedThread] = {}
    _lock = threading.Lock()

    @classmethod
    def register(cls, thread: ManagedThread) -> None:
        with cls._lock:
            cls._threads[thread.name] = thread

    @classmethod
    def unregister(cls, name: str) -> None:
        with cls._lock:
            cls._threads.pop(name, None)

    @classmethod
    def get(cls, name: str) -> ManagedThread | None:
        with cls._lock:
            return cls._threads.get(name)

    @classmethod
    def all(cls) -> list[ManagedThread]:
        with cls._lock:
            return list(cls._threads.values())

    @classmethod
    def snapshot(cls) -> list[ThreadStatus]:
        return [thread.status() for thread in cls.all()]


# An explicit, human-readable instance id set by the application (via
# orcha.core.initialise). When set, it is used instead of the hostname:pid
# default so the UI shows meaningful names (e.g. 'scheduler', 'workspace').
_configured_instance_id: str | None = None


def set_instance_id(instance_id: str | None) -> None:
    """
    Set the instance id this process reports its thread health under. Call before
    any managed threads start (``orcha.core.initialise`` does this from
    ``application_name``).

    Choosing a *stable* id (e.g. the application/role name) rather than the
    default ``hostname:pid`` means a restart reuses the same id, so the
    supervisor overwrites the old rows instead of leaving a ghost "offline"
    instance behind until it is pruned. The trade-off: if you run multiple
    processes with the *same* id (e.g. several task-runner replicas on one host)
    they will clobber each other's rows - give those distinct ids.
    """
    global _configured_instance_id
    _configured_instance_id = instance_id


def _default_instance_id() -> str:
    """
    Fallback identity when none was configured via :func:`set_instance_id`:
    hostname + pid. Unique per process, but changes on every restart (which is
    what leaves ghost rows behind), so prefer configuring a stable id.
    """
    return f'{socket.gethostname()}:{os.getpid()}'


class ThreadSupervisor:
    """
    A single watchdog per process. It:
    - restarts any ``ManagedThread`` that has died and is set to auto-restart
      (a safety net on top of the per-tick exception handling), and
    - periodically persists a snapshot of all thread health to the database so
      other processes (the UI) can observe it.

    Use ``ThreadSupervisor.ensure_running()`` to lazily create and start the
    singleton; ``ManagedThread.start()`` calls this automatically so in normal
    use nothing needs to wire it up explicitly.
    """
    _instance: 'ThreadSupervisor | None' = None
    _lock = threading.Lock()

    # How long a health row can go un-updated before other instances should be
    # treated as offline and (eventually) pruned.
    prune_age = td(days=1)

    def __init__(self, instance_id: str, check_interval: float = 10.0):
        self.instance_id = instance_id
        self.check_interval = check_interval
        self._mt = ManagedThread(
            name='__thread_supervisor__',
            group='supervisor',
            tick=self._supervise,
            interval=check_interval,
            heartbeat_timeout=check_interval * 6,
            auto_restart=False,
            ensure_supervisor=False,
        )
        self._cleaned_startup = False

    @classmethod
    def ensure_running(cls, instance_id: str | None = None) -> 'ThreadSupervisor':
        with cls._lock:
            if cls._instance is None:
                cls._instance = ThreadSupervisor(
                    instance_id=instance_id or _configured_instance_id or _default_instance_id()
                )
                cls._instance._mt.start()
                _safe_log(
                    'supervisor_started', 'Thread supervisor started',
                    {'instance_id': cls._instance.instance_id}
                )
            return cls._instance

    def _supervise(self) -> None:
        for thread in ThreadRegistry.all():
            if thread is self._mt:
                continue
            status = thread.status()
            if (
                status.state == ThreadState.crashed.value
                and thread.auto_restart
                and not thread.is_stopping()
            ):
                _safe_log(
                    'crash_detected',
                    f'Detected dead thread {thread.name}, restarting',
                    {'name': thread.name, 'group': thread.group}
                )
                thread.restart()
        self._persist()

    def _persist(self) -> None:
        """
        Write the current snapshot to the database. Replaces all rows for this
        instance each cycle (so unregistered/renamed threads don't linger) and
        prunes stale rows left behind by long-gone instances.
        """
        if not is_configured():
            return
        now = current_time()
        snapshot = ThreadRegistry.snapshot()
        try:
            with session_maker.begin() as session:
                # Replace this instance's rows wholesale - cheap (a handful of
                # rows) and guarantees no ghost entries for this instance.
                session.query(ThreadHealthRecord).filter_by(
                    instance_id=self.instance_id
                ).delete()
                for st in snapshot:
                    session.add(ThreadHealthRecord(
                        instance_id=self.instance_id,
                        thread_name=st.name,
                        thread_group=st.group,
                        state=st.state,
                        interval_s=st.interval_s,
                        heartbeat_timeout_s=st.heartbeat_timeout_s,
                        started_at=st.started_at,
                        last_heartbeat=st.last_heartbeat,
                        last_tick_at=st.last_tick_at,
                        updated_at=now,
                        restart_count=st.restart_count,
                        error_count=st.error_count,
                        consecutive_errors=st.consecutive_errors,
                        last_error=st.last_error,
                        last_error_at=st.last_error_at,
                    ))
                # Prune rows from instances that have stopped reporting.
                session.query(ThreadHealthRecord).filter(
                    ThreadHealthRecord.updated_at < now - self.prune_age
                ).delete()
        except Exception as e:
            # Never let persistence problems take down the supervisor loop.
            _safe_log(
                'persist_error', 'Failed to persist thread health snapshot',
                {'error': f'{type(e).__name__}: {e}'}
            )


# ---------------------------------------------------------------------------
# Database persistence
# ---------------------------------------------------------------------------

# ORM record class mapped onto the canonical table in orcha.core.tables; the
# shared engine/session_maker live in orcha.core.database. The orcha schema and
# this table are created and owned by the migrations in orcha.migrations (run
# `alembic upgrade head`); they are not built here. Persistence is a no-op until
# orcha.core.initialise() has configured the shared connection.
class ThreadHealthRecord(Base):
    __table__ = tables.thread_health


def get_health_snapshot() -> list[dict]:
    """
    Read the persisted thread-health snapshot from the database. Returns a list
    of plain dicts (one per thread per instance) for the UI/query layer to
    consume. Returns an empty list if the shared connection isn't configured.
    """
    if not is_configured():
        return []
    with session_maker.begin() as session:
        rows = session.query(ThreadHealthRecord).all()
        return [
            {
                'instance_id': row.instance_id,
                'thread_name': row.thread_name,
                'thread_group': row.thread_group,
                'state': row.state,
                'interval_s': row.interval_s,
                'heartbeat_timeout_s': row.heartbeat_timeout_s,
                'started_at': row.started_at,
                'last_heartbeat': row.last_heartbeat,
                'last_tick_at': row.last_tick_at,
                'updated_at': row.updated_at,
                'restart_count': row.restart_count,
                'error_count': row.error_count,
                'consecutive_errors': row.consecutive_errors,
                'last_error': row.last_error,
                'last_error_at': row.last_error_at,
            }
            for row in rows
        ]
