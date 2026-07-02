from __future__ import annotations

import json
import warnings
from dataclasses import dataclass
from datetime import datetime as dt
from datetime import timedelta as td
from enum import Enum

from orcha import current_time
from orcha.core import monitors, tables
from orcha.core.database import Base, session_maker
from orcha.core.monitors import AlertBase, AlertOutputType, MonitorBase
from orcha.core.tasks import RunItem, TaskItem, VersionMismatchException
from orcha.core.thread_monitor import ManagedThread
from orcha.utils.log import LogManager
from orcha.utils.mqueue import Channel, Message, Producer

scheduler_log = LogManager('scheduler')


# ORM record class mapped onto the single-source-of-truth table in
# orcha.core.tables; the shared engine/session_maker live in orcha.core.database.
# The orcha schema and the schedulers table are created and owned by the Alembic
# migrations in orcha.migrations (run `alembic upgrade head`); not built here.
class SchedulerRecord(Base):
    __table__ = tables.schedulers


class RunningState(Enum):
    """
    The running state of the scheduler.
    - running: The scheduler is running and creating runs.
    - stopped: The scheduler has been stopped.
    - paused: Not currently used.
    """
    running = 'running'
    stopped = 'stopped'
    paused = 'paused'


class MqueueChannels():
    """
    This class is used to define the channels and message types
    for the scheduler monitor.
    """

    class _HistoricalRunMessage:
        def __init__(self, scheduler_id: str, task_id: str, run_id: str, note: str):
            self.scheduler_id = scheduler_id
            self.task_id = task_id
            self.run_id = run_id
            self.note = note

        def to_json(self) -> str:
            return json.dumps({
                "scheduler_id": self.scheduler_id,
                "task_id": self.task_id,
                "run_id": self.run_id,
                "note": self.note
            })

        @classmethod
        def from_json(cls, json_str: str):
            data = json.loads(json_str)
            return cls(
                scheduler_id=data["scheduler_id"],
                task_id=data["task_id"],
                run_id=data["run_id"],
                note=data["note"]
            )

    historical_run = Channel(
        name='scheduler_historical_run',
        message_type=_HistoricalRunMessage
    )

    class _InactiveSchedulerMessage:
        def __init__(self, scheduler_id: str):
            self.scheduler_id = scheduler_id

        def to_json(self) -> str:
            return json.dumps({
                "scheduler_id": self.scheduler_id
            })

        @classmethod
        def from_json(cls, json_str: str):
            data = json.loads(json_str)
            return cls(
                scheduler_id=data["scheduler_id"]
            )

    inactive_scheduler = Channel(
        name='inactive_scheduler',
        message_type=_InactiveSchedulerMessage
    )

    class _InactiveTaskMessage:
        def __init__(self, scheduler_id: str, task_id: str):
            self.scheduler_id = scheduler_id
            self.task_id = task_id

        def to_json(self) -> str:
            return json.dumps({
                "scheduler_id": self.scheduler_id,
                "task_id": self.task_id
            })

        @classmethod
        def from_json(cls, json_str: str):
            data = json.loads(json_str)
            return cls(
                scheduler_id=data["scheduler_id"],
                task_id=data["task_id"]
            )

    inactive_task = Channel(
        name='inactive_task',
        message_type=_InactiveTaskMessage
    )


    class _SchedulerStartedMessage:
        def __init__(self, scheduler_id: str):
            self.scheduler_id = scheduler_id

        def to_json(self) -> str:
            return json.dumps({
                "scheduler_id": self.scheduler_id
            })

        @classmethod
        def from_json(cls, json_str: str):
            data = json.loads(json_str)
            return cls(
                scheduler_id=data["scheduler_id"]
            )

    scheduler_started = Channel(
        name='scheduler_started',
        message_type=_SchedulerStartedMessage
    )


class SchedulerMonitor(MonitorBase):
    """
    This class is used to monitor the scheduler and alert on any
    'failed' type events such as disabled tasks, inactive tasks, etc.
    Also uses an external thread to check the status of the scheduler:
    - If the scheduler has been inactive for over 5 minutes.
    """

    def __init__(
            self,
            alert: AlertBase,
            schedulers: list[Scheduler] | None = None,
            max_alerts: int = 5
        ):
        """
        Initialise the scheduler monitor with the given alert class. This
        monitor alerts when:
        - A scheduler is inactive
        - A task is inactive
        - A historical run has failed
        - A scheduler has started
        ### Args
        - scheduler(Scheduler): The scheduler to monitor.
        - alert(AlertBase): The alert class to use for sending alerts.
        - max_alerts(int = 5): The maximum number of alerts to send.
        """
        self.alert = alert
        self.max_alerts = max_alerts
        # Build a fresh list per instance; a shared mutable default would be
        # mutated by add_scheduler across all monitors constructed without one.
        self.schedulers = schedulers if schedulers is not None else []

        super().__init__(
            alert=alert,
            monitor_name='scheduler_monitor',
            message_channel=[
                MqueueChannels.inactive_scheduler,
                MqueueChannels.inactive_task,
                MqueueChannels.historical_run,
                MqueueChannels.scheduler_started
            ],
            check_function=self.check
        )

    def _run_to_ui_url(self, run_id: str) -> str:
        if monitors.MONITOR_CONFIG and monitors.MONITOR_CONFIG.orcha_ui_base_url:
            href = f'{monitors.MONITOR_CONFIG.orcha_ui_base_url}/run_details?run_id={run_id}'
            run_href = f'<a href="{href}">{run_id}</a>'
            return run_href
        return run_id

    def _task_to_ui_url(self, task_id: str) -> str:
        if monitors.MONITOR_CONFIG and monitors.MONITOR_CONFIG.orcha_ui_base_url:
            href = f'{monitors.MONITOR_CONFIG.orcha_ui_base_url}/task_details?task_id={task_id}'
            task_href = f'<a href="{href}">{task_id}</a>'
            return task_href
        return task_id

    def add_scheduler(self, scheduler: Scheduler):
        """
        Add a scheduler to the monitor.
        """
        self.schedulers.append(scheduler)

    def check(self, channel: Channel, message: Message):
        """
        Check the scheduler for any issues and send alerts if required.
        """

        message_scheduler_id = getattr(message, 'scheduler_id')
        if not message_scheduler_id:
            raise Exception('Message does not have a scheduler_id')

        if message_scheduler_id not in [s.scheduler_idk for s in self.schedulers]:
            return

        if isinstance(message, MqueueChannels._InactiveSchedulerMessage):
            if self.alert.output_type == AlertOutputType.HTML:
                self.alert.send_alert(f'''
                    <b>Inactive Scheduler Alert</b>
                    <br>
                    <br><b>Scheduler ID</b>
                    <br>{message.scheduler_id}
                    <br>
                    <br>Scheduler has been inactive for over 5 minutes. Please
                    check the scheduler to ensure it's running correctly.
                ''')
            else:
                self.alert.send_alert(f'''
                    Inactive Scheduler Alert
                    Scheduler ID: {message.scheduler_id}
                    Scheduler has been inactive for over 5 minutes. Please
                    check the scheduler to ensure it's running correctly.
                ''')
        elif isinstance(message, MqueueChannels._InactiveTaskMessage):
            if self.alert.output_type == AlertOutputType.HTML:
                self.alert.send_alert(f'''
                    <b>Inactive Task Alert</b>
                    <br>
                    <br><b>Task ID</b>
                    <br>{self._task_to_ui_url(message.task_id)}
                    <br>
                    <br>Task task has been disabled due to inactivity. Please
                    check the task runner and re-enable the task if required.
                ''')
            else:
                self.alert.send_alert(f'''
                    Inactive Task Alert
                    Task ID: {message.task_id}
                    Task task has been disabled due to inactivity. Please
                    check the task runner and re-enable the task if required.
                ''')
        elif isinstance(message, MqueueChannels._HistoricalRunMessage):
            if self.alert.output_type == AlertOutputType.HTML:
                self.alert.send_alert(f'''
                    <b>Historical Run Alert</b>
                    <br>
                    <br><b>Task ID</b>
                    <br>{self._task_to_ui_url(message.task_id)}
                    <br><b>Run ID</b>
                    <br>{self._run_to_ui_url(message.run_id)}
                    <br><b>Note</b>
                    <br>{message.note}
                ''')
            else:
                self.alert.send_alert(f'''
                    Historical Run Alert
                    Task ID: {message.task_id}
                    Run ID: {message.run_id}
                    Note: {message.note}
                ''')
        elif isinstance(message, MqueueChannels._SchedulerStartedMessage):
            if self.alert.output_type == AlertOutputType.HTML:
                self.alert.send_alert(f'''
                    <b>Scheduler Started Alert</b>
                    <br>
                    <br><b>Scheduler ID</b>
                    <br>{message.scheduler_id}
                    <br>
                    <br>Scheduler has been started. This typically happens when
                    Orcha starts up.
                ''')
            else:
                self.alert.send_alert(f'''
                    Scheduler Started Alert
                    Scheduler ID: {message.scheduler_id}
                    Scheduler has been started. This typically happens when
                    Orcha starts up.
                ''')


@dataclass
class OrchaSchedulerConfig:
        """
        This class is used to store the configuration for the orcha scheduler.

        ### Options
        - scheduler_idk(str = 'main'): The identity of this scheduler. Each running scheduler must use a distinct id so they don't clobber each other's row in the schedulers table. Defaults to 'main' for the single-scheduler baseline.
        - task_refresh_interval(float = 30): The interval in seconds at which the scheduler will reload the task list from the database.
        - fail_unstarted_runs(bool = True): If True, then when a run is due, but the last run didn't start, then the last run will be set to failed before a new run is created.
        - disable_stale_tasks(bool = True): If True, then when a task hasn't been active since the last run, then the task will be set to inactive.
        - prune_runs_max_age(td | None = td(days=180)): The maximum age of runs to keep in the database. If None, then no runs will be pruned.
        - prune_logs_max_age(td | None = td(days=180)): The maximum age of logs to keep in the database. If None, then no logs will be pruned.
        - prune_interval(float = 3600): The interval in seconds at which the scheduler will prune the runs and logs.
        - fail_historical_runs(bool = True): If True, fail any unstarted/incomplete runs that are older than fail_historical_age.
        - fail_historical_age(td | None = td(hours=6)): The age at which an unstarted run should be failed.
        - fail_historical_interval(float = 180): The interval in seconds at which the scheduler will check.
        """
        scheduler_idk: str = 'main'
        task_refresh_interval: float = 30
        fail_unstarted_runs: bool = True
        disable_stale_tasks: bool = True
        prune_runs_max_age: td | None = td(days=180)
        prune_logs_max_age: td | None = td(days=180)
        prune_interval: float = 3600
        fail_historical_runs: bool = True
        fail_historical_age: td | None = td(hours=6)
        fail_historical_interval: float = 180


class Scheduler:
    """
    The scheduler creates threads and creates runs in the database for
    tasks that are due to run and other maintenance activities.
    The scheduler can be run in a separate or the same environment as the
    task runner.
    """

    def __init__(
            self,
            config: OrchaSchedulerConfig = OrchaSchedulerConfig(),
            monitors: list[SchedulerMonitor] | None = None,
            fail_unstarted_runs: bool | None = None,
            disable_stale_tasks: bool | None = None,
        ):
        """
        Initialise the scheduler with the given settings.
        ### Args
        - config(OrchaSchedulerConfig | None = None): The configuration for the scheduler.
        - monitors(list[SchedulerMonitor] = []): A list of monitors to add to the scheduler.
        - fail_unstarted_runs: If True, then if a run is due, but the last
        run didn't start, then the last run will be set to failed before a new
        run is created.
        - disable_stale_tasks: If True, then if a task hasn't been active
        since the last run, then the task will be set to inactive.
        """
        self.all_tasks = []

        self.scheduler_idk = config.scheduler_idk

        # Bind the scheduler to the monitors
        for monitor in (monitors or []):
            monitor.add_scheduler(self)

        self.running_state: RunningState = RunningState.running
        # Long-lived background loops are now run as supervised ManagedThreads
        # (see orcha.core.thread_monitor). They are created lazily in start()
        # and tracked here so start()/stop() remain idempotent.
        self.thread: ManagedThread | None = None
        self.prune_thread: ManagedThread | None = None
        self.fail_hist_thread: ManagedThread | None = None
        self.refresh_tasks_thread: ManagedThread | None = None
        self.last_active_thread: ManagedThread | None = None

        self.task_refresh_interval = config.task_refresh_interval
        self.fail_unstarted_runs = config.fail_unstarted_runs
        self.disable_stale_tasks = config.disable_stale_tasks
        self.prune_runs_max_age = config.prune_runs_max_age
        self.prune_logs_max_age = config.prune_logs_max_age
        self.prune_interval = config.prune_interval
        self.fail_historical_runs = config.fail_historical_runs
        self.fail_historical_age = config.fail_historical_age
        self.fail_historical_interval = config.fail_historical_interval

        # Overwrite the config with the deprecated parameters
        if fail_unstarted_runs is not None:
            warnings.warn(
                'The fail_unstarted_runs parameter is deprecated. Use the OrchaSchedulerConfig class instead.',
                DeprecationWarning,
                stacklevel=2,
            )
            self.fail_unstarted_runs = fail_unstarted_runs
        if disable_stale_tasks is not None:
            warnings.warn(
                'The disable_stale_tasks parameter is deprecated. Use the OrchaSchedulerConfig class instead.',
                DeprecationWarning,
                stacklevel=2,
            )
            self.disable_stale_tasks = disable_stale_tasks

        # Start the last active check thread. Previously a bare, un-stoppable
        # `while True` thread; now a supervised ManagedThread that can be
        # stopped and is restarted automatically if it dies.
        self.last_active_thread = ManagedThread(
            name=f'{self.scheduler_idk}:scheduler:check_last_active',
            group='scheduler',
            tick=self._tick_check_last_active,
            interval=120,
            heartbeat_timeout=300,
        )
        self.last_active_thread.start()

        Scheduler.set_loaded_at(self.scheduler_idk)
        scheduler_log.add_entry(
            actor='scheduler', category='status', text='Scheduler Initialised', json={
                'scheduler_idk': self.scheduler_idk
            }
        )

    @staticmethod
    def set_loaded_at(scheduler_idk: str = 'main'):
        """
        Set the loaded_at time for the scheduler in the database.
        """
        with session_maker.begin() as session:
            session.merge(
                SchedulerRecord(scheduler_idk=scheduler_idk, loaded_at=current_time())
            )

    @staticmethod
    def get_loaded_at(scheduler_idk: str = 'main'):
        """
        Get the loaded_at time for the scheduler from the database.
        """
        with session_maker.begin() as session:
            record = session.query(SchedulerRecord
                ).filter_by(scheduler_idk=scheduler_idk
                ).first()
            if record is not None:
                if hasattr(record, 'loaded_at'):
                    # TODO fix this type hinting
                    data: dt = record.loaded_at # type: ignore
                    return data

    @staticmethod
    def get_last_active(scheduler_idk: str = 'main'):
        """
        Get the last_active time for the scheduler from the database.
        """
        with session_maker.begin() as session:
            record = session.query(SchedulerRecord
                ).filter_by(scheduler_idk=scheduler_idk
                ).first()
            if record is not None:
                if hasattr(record, 'last_active'):
                    # TODO fix this type hinting
                    data: dt = record.last_active # type: ignore
                    return data

    def _tick_check_last_active(self):
        """
        One iteration of the last-active check: send a message if the scheduler
        has been inactive for over 5 minutes. Run on an interval by a
        ManagedThread.
        """
        # If the scheduler hasn't been run/no last active
        # then we don't want to send a message - mostly to avoid
        # sending a message on the first run and at startup
        last_active = self.get_last_active(self.scheduler_idk)
        if last_active is not None:
            # if it's over 10 minutes since the last active time
            # then assume roughly 5 alerts have been sent and stop
            if last_active < current_time() - td(minutes=10):
                return
            elif last_active < current_time() - td(minutes=5):
                Producer().send_message(
                    channel=MqueueChannels.inactive_scheduler,
                    message=MqueueChannels.inactive_scheduler.message_type(
                        scheduler_id=self.scheduler_idk
                    )
                )

    def update_active(self):
        """
        Update the last_active time for the scheduler in the database.
        """
        with session_maker.begin() as session:
            session.merge(
                SchedulerRecord(scheduler_idk=self.scheduler_idk, last_active=current_time())
            )
            self.last_refresh = current_time()

    def start(self):
        """
        This starts the scheduler threads, and is safe to call even if the
        threads are already running. If the threads are already running, then
        this will do nothing.
        """
        scheduler_log.add_entry(
            actor='scheduler', category='status', text='Starting', json={}
        )
        Producer().send_message(
            channel=MqueueChannels.scheduler_started,
            message=MqueueChannels.scheduler_started.message_type(
                scheduler_id=self.scheduler_idk
            )
        )
        self.running_state = RunningState.running
        # All loops gate their work on the scheduler being in the 'running'
        # state, so they keep heartbeating (but idle) when stopped/paused.
        is_running = lambda: self.running_state == RunningState.running

        # ManagedThread.start() is idempotent (no-op if already alive) and the
        # supervisor restarts any loop that dies, so we simply (re)create and
        # start each loop here.
        if self.thread is None:
            self.thread = ManagedThread(
                name=f'{self.scheduler_idk}:scheduler:process_schedules',
                group='scheduler',
                tick=self._tick_process_schedules,
                interval=15,
                heartbeat_timeout=300,
            )
        self.thread.start()

        # Start the run pruning thread
        if self.prune_runs_max_age is not None:
            if self.prune_thread is None:
                self.prune_thread = ManagedThread(
                    name=f'{self.scheduler_idk}:scheduler:prune',
                    group='scheduler',
                    tick=self._tick_prune,
                    interval=self.prune_interval,
                    startup_delay=self.prune_interval,
                    run_condition=is_running,
                    # Pruning is infrequent and can legitimately be slow, so we
                    # don't flag stale heartbeats for it.
                    heartbeat_timeout=None,
                )
            self.prune_thread.start()

        # Start the historical run failure thread
        if self.fail_historical_runs:
            if self.fail_hist_thread is None:
                self.fail_hist_thread = ManagedThread(
                    name=f'{self.scheduler_idk}:scheduler:fail_historical',
                    group='scheduler',
                    tick=self._tick_fail_historical,
                    interval=self.fail_historical_interval,
                    startup_delay=60,
                    run_condition=is_running,
                    heartbeat_timeout=None,
                )
            self.fail_hist_thread.start()

        # Start the task refreshing thread
        if self.refresh_tasks_thread is None:
            self.refresh_tasks_thread = ManagedThread(
                name=f'{self.scheduler_idk}:scheduler:refresh_tasks',
                group='scheduler',
                tick=self._tick_refresh_tasks,
                interval=self.task_refresh_interval,
                startup_delay=self.task_refresh_interval,
                run_condition=is_running,
                heartbeat_timeout=max(self.task_refresh_interval * 4, 120),
            )
        self.refresh_tasks_thread.start()
        return self.thread

    def stop(self):
        """
        This stops the scheduler threads, and is safe to call even if the
        threads are already stopped. If the threads are already stopped, then
        this will do nothing. This will block until the threads have stopped.
        """
        scheduler_log.add_entry(
            actor='scheduler', category='status', text='Stopping', json={}
        )
        self.running_state = RunningState.stopped
        for managed in (
            self.thread,
            self.prune_thread,
            self.fail_hist_thread,
            self.refresh_tasks_thread,
            self.last_active_thread,
        ):
            if managed is not None:
                managed.stop()

    def pause(self):
        """
        Not implemented yet.
        """
        raise NotImplementedError('Pausing the scheduler is not implemented yet.')

    def _tick_prune(self):
        """
        One iteration of run/log pruning. Run on an interval by a ManagedThread
        and gated on the scheduler being in the running state.
        """
        if self.prune_runs_max_age is not None:
            for task in self.all_tasks:
                del_count = task.prune_runs(self.prune_runs_max_age)
                scheduler_log.add_entry(
                    actor='scheduler', category='prune_runs', text='Pruning runs',
                    json={
                        'task_id': task.task_idk,
                        'max_age': str(self.prune_runs_max_age),
                        'deleted_count': del_count
                    }
                )
        if self.prune_logs_max_age is not None:
            del_count = scheduler_log.prune(self.prune_logs_max_age)
            scheduler_log.add_entry(
                actor='scheduler', category='prune_logs', text='Pruning logs',
                json={
                    'max_age': str(self.prune_logs_max_age),
                    'deleted_count': del_count
                }
            )

    def _tick_fail_historical(self):
        """
        One iteration of historical run failure handling. This will fail:
        - Runs that were scheduled or started but didn't finish within the time
        - Runs that have been inactive for over 5 minutes

        The ManagedThread applies a startup delay so that, when the scheduler is
        started in the same environment as the task runner, the runner has time
        to start and load tasks before the first check.
        """
        if not self.fail_historical_runs or self.fail_historical_age is None:
            return
        for task in self.all_tasks:
            open_runs = task.get_running_runs() + task.get_queued_runs()
            historical_count = 0
            for run in open_runs:
                run_age = current_time() - run.scheduled_time
                if run_age > self.fail_historical_age:
                    run.set_status(
                        status='failed',
                        output={
                            'message': 'Historical run failed to start/finish'
                        },
                        send_alert=False
                    )
                    run.set_progress(
                        progress='complete',
                        zero_duration=True,
                    )
                    Producer().send_message(
                        channel=MqueueChannels.historical_run,
                        message=MqueueChannels.historical_run.message_type(
                            scheduler_id=self.scheduler_idk,
                            task_id=task.task_idk,
                            run_id=run.run_idk,
                            note='Historical run failed to start/finish'
                        )
                    )
                    historical_count += 1
                elif run.progress == 'running':
                    if run.last_active is not None:
                        if run.last_active < current_time() - td(minutes=5):
                            run.set_status(
                                status='failed',
                                output={
                                    'message': 'Run has been inactive for over 5 minutes'
                                },
                                send_alert=False
                            )
                            run.set_progress(
                                progress='complete',
                                zero_duration=True,
                            )
                            Producer().send_message(
                                channel=MqueueChannels.historical_run,
                                message=MqueueChannels.historical_run.message_type(
                                    scheduler_id=self.scheduler_idk,
                                    task_id=task.task_idk,
                                    run_id=run.run_idk,
                                    note='Run has been inactive for over 5 minutes'
                                )
                            )
                            historical_count += 1
            scheduler_log.add_entry(
                actor='scheduler', category='fail_historical_runs',
                text='Failing historical runs',
                json={
                    'task_id': task.task_idk,
                    'max_age': str(self.fail_historical_age),
                    'failed_count': historical_count
                }
            )

    def _tick_refresh_tasks(self):
        """One iteration of reloading the task list from the database."""
        self.all_tasks = TaskItem.get_all()
        scheduler_log.add_entry(
            actor='scheduler', category='refresh_tasks',
            text='Refreshing tasks',
            json={'task_count': len(self.all_tasks)}
        )

    def _supersede_pending_runs(self, task: TaskItem, schedule) -> None:
        """
        Fail any still-pending (unstarted+queued) run for this task/schedule so
        that a newer scheduled run can take its place (the single-pending-run
        policy). Each superseded run is recorded as a failure (with an alert, so
        it counts toward FailedRunsMonitor's disable-after-N path).

        Only runs that are genuinely still unstarted+queued are failed: if a
        runner has just claimed a run (moving it to running), the optimistic
        version check raises and we skip it -- we never fail a run a runner is
        already executing.
        """
        for pending in RunItem.get_all_queued(task=task, schedule=schedule):
            try:
                pending.set_status(
                    status='failed',
                    output={'message': 'superseded by newer scheduled run'},
                    send_alert=True,
                )
                # Mark it complete (as the historical-fail path does) so the run
                # doesn't linger as 'failed' but still 'queued'. zero_duration
                # keeps its (never-started) duration at zero.
                pending.set_progress(
                    progress='complete',
                    zero_duration=True,
                )
            except VersionMismatchException:
                # The run was claimed by a runner between our read and write, so
                # it is no longer pending; leave it for the runner to complete.
                continue

    def _tick_process_schedules(self):
        """
        One iteration of the main scheduling loop: update the scheduler's active
        time and create runs for any tasks that are due. The active-time update
        and logging happen every tick (the scheduler heartbeat), while run
        creation only happens while running.
        """
        # log that we're processing schedules and log which tasks
        scheduler_log.add_entry(
            actor='main_loop', category='status', text='Processing schedules',
            json={
                'task_count': len(self.all_tasks),
                'task_names': ', '.join([task.task_idk for task in self.all_tasks])
            }
        )
        self.update_active()
        # Only create runs if we're running (this loop isn't run_condition
        # gated so the heartbeat above always ticks)
        if self.running_state != RunningState.running:
            return

        if len(self.all_tasks) == 0:
            self.all_tasks = TaskItem.get_all()

        for task in self.all_tasks:
            # Only check enabled tasks (e.g. no disabled/inactive tasks)
            if task.status != 'enabled':
                continue
            for schedule in task.schedule_sets:
                is_due, last_run = task.is_run_due_with_last(schedule)
                if is_due:
                    if self.disable_stale_tasks and last_run is not None:
                        # If the task hasn't been active since the last run,
                        # then it's stale and should be disabled.
                        # Tasks should be checked every 5s, and runs at most frequent, every 1 minute
                        # so a task should have been active many times since the last run
                        if task.last_active < min(last_run.scheduled_time, current_time() - td(minutes=5)):
                            task.set_status('inactive', 'Task has been inactive since last scheduled run')
                            Producer().send_message(
                                channel=MqueueChannels.inactive_task,
                                message=MqueueChannels.inactive_task.message_type(
                                    scheduler_id=self.scheduler_idk,
                                    task_id=task.task_idk
                                )
                            )
                            continue
                    # Single-pending-run policy: a schedule should have at most
                    # one pending (unstarted+queued) run. Before creating the new
                    # run, supersede any prior pending run for this schedule by
                    # failing it. Repeated supersessions then legitimately trip
                    # FailedRunsMonitor's disable-after-N path, surfacing a task
                    # that never gets picked up as an error.
                    self._supersede_pending_runs(task, schedule)
                    # print('Run due for task:', task.task_idk)
                    run = task.schedule_run(
                        schedule=schedule,
                        schedule_by_id=self.scheduler_idk
                    )
                    if run is None:
                        raise Exception('Failed to create run')