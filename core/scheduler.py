from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime as dt
from datetime import timedelta as td
from enum import Enum

from sqlalchemy import Column, DateTime, String
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Session, sessionmaker

from orcha.core import monitors
from orcha.core.monitors import AlertBase, AlertOutputType, MonitorBase
from orcha.core.tasks import TaskItem
from orcha.utils.log import LogManager
from orcha.utils.mqueue import Channel, Message, Producer
from orcha.utils.sqlalchemy import postgres_scaffold, sqlalchemy_build

scheduler_log = LogManager('scheduler')

Base: DeclarativeMeta
engine: Engine
s_maker: sessionmaker[Session]


def _setup_sqlalchemy(
        orcha_user: str, orcha_pass: str,
        orcha_server: str, orcha_db: str,
        orcha_schema: str, application_name: str
    ):
    global is_initialised, Base, engine, s_maker, SchedulerRecord
    is_initialised = True
    Base, engine, s_maker = postgres_scaffold(
        user=orcha_user,
        passwd=orcha_pass,
        server=orcha_server,
        db=orcha_db,
        schema=orcha_schema,
        application_name=application_name
    )

    class SchedulerRecord(Base):
        __tablename__ = 'schedulers'

        scheduler_idk = Column(String, primary_key=True)
        last_active = Column(DateTime(timezone=False))
        loaded_at = Column(DateTime(timezone=False))

    sqlalchemy_build(Base, engine, orcha_schema)


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
            schedulers: list[Scheduler] = [],
            max_alerts: int = 5
        ):
        """
        Initialise the scheduler monitor with the given alert class.
        ### Args
        - scheduler(Scheduler): The scheduler to monitor.
        - alert(AlertBase): The alert class to use for sending alerts.
        - max_alerts(int = 5): The maximum number of alerts to send.
        """
        self.alert = alert
        self.max_alerts = max_alerts
        self.schedulers = schedulers

        super().__init__(
            alert=alert,
            monitor_name='scheduler_monitor',
            message_channel=[
                MqueueChannels.inactive_scheduler,
                MqueueChannels.inactive_task,
                MqueueChannels.historical_run
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


@dataclass
class OrchaSchedulerConfig:
        """
        This class is used to store the configuration for the orcha scheduler.

        ### Options
        - task_refresh_interval(float = 60): The interval in seconds at which the scheduler will reload the task list from the database.
        - fail_unstarted_runs(bool = True): If True, then when a run is due, but the last run didn't start, then the last run will be set to failed before a new run is created.
        - disable_stale_tasks(bool = True): If True, then when a task hasn't been active since the last run, then the task will be set to inactive.
        - prune_runs_max_age(td | None = 180): The maximum age of runs to keep in the database. If None, then no runs will be pruned.
        - prune_logs_max_age(td | None = 180): The maximum age of logs to keep in the database. If None, then no logs will be pruned.
        - prune_interval(float = 3600): The interval in seconds at which the scheduler will prune the runs and logs.
        - fail_historical_runs(bool = True): If True, fail any unstarted/incomplete runs that are older than fail_historical_age.
        - fail_historical_age(td | None = 6): The age in hours when an unstarted run should be failed.
        - fail_historical_interval(float = 180): The interval in seconds at which the scheduler will check.
        """
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
            monitors: list[SchedulerMonitor] = [],
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

        self.scheduler_idk = 'main'

        # Bind the scheduler to the monitors
        for monitor in monitors:
            monitor.add_scheduler(self)

        self.running_state: RunningState = RunningState.running
        self.thread = None
        self.prune_thread = None
        self.fail_hist_thread = None
        self.refresh_tasks_thread = None

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
            self.fail_unstarted_runs = fail_unstarted_runs
            raise DeprecationWarning('The fail_unstarted_runs parameter is deprecated. Use the OrchaSchedulerConfig class instead.')
        if disable_stale_tasks is not None:
            self.disable_stale_tasks = disable_stale_tasks
            raise DeprecationWarning('The disable_stale_tasks parameter is deprecated. Use the OrchaSchedulerConfig class instead.')

        # Start the last active check thread
        threading.Thread(
            target=self._thread_helper_check_last_active
        ).start()

        # TODO Move to using scheduler_idks as if we run multiple schedulers
        # we'll only record when the latest one started
        Scheduler.set_loaded_at()

    @staticmethod
    def set_loaded_at(scheduler_idk: str = 'main'):
        """
        Set the loaded_at time for the scheduler in the database.
        """
        with s_maker.begin() as session:
            # Using a single scheduler for now
            session.merge(
                SchedulerRecord(scheduler_idk='main', loaded_at=dt.now())
            )

    @staticmethod
    def get_loaded_at(scheduler_idk: str = 'main'):
        """
        Get the loaded_at time for the scheduler from the database.
        """
        with s_maker.begin() as session:
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
        with s_maker.begin() as session:
            record = session.query(SchedulerRecord
                ).filter_by(scheduler_idk=scheduler_idk
                ).first()
            if record is not None:
                if hasattr(record, 'last_active'):
                    # TODO fix this type hinting
                    data: dt = record.last_active # type: ignore
                    return data

    def _thread_helper_check_last_active(self):
        """
        This is the thread function that will check the last active time
        of the scheduler and send a messages if it's been inactive for over
        5 minutes.
        """
        while True:
            # If the scheduler hasn't been run/no last active
            # then we don't want to send a message - mostly to avoid
            # sending a message on the first run and at startup
            time.sleep(120)
            last_active = self.get_last_active()
            if last_active is not None:
                # if it's over 10 minutes since the last active time
                # then assume roughly 5 alerts have been sent and stop
                if last_active < dt.now() - td(minutes=10):
                    continue
                elif last_active < dt.now() - td(minutes=5):
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
        with s_maker.begin() as session:
            # Using a single scheduler for now
            session.merge(
                SchedulerRecord(scheduler_idk='main', last_active=dt.now())
            )
            self.last_refresh = dt.now()

    def start(self):
        """
        This starts the scheduler threads, and is safe to call even if the
        threads are already running. If the threads are already running, then
        this will do nothing.
        """
        scheduler_log.add_entry(
            actor='scheduler', category='status', text='Starting', json={}
        )
        self.running_state = RunningState.running
        # Only start threads if they are None (dont exist) or they are no
        # longer alive (have finished/died/stopped)
        if self.thread is None or not self.thread.is_alive():
            # Start the run scheduling thread
            self.thread = threading.Thread(target=self._process_schedules)
            self.thread.start()
        # Start the run pruning thread
        if self.prune_runs_max_age is not None:
            if self.prune_thread is None or not self.prune_thread.is_alive():
                self.prune_thread = threading.Thread(target=self._prune_runs_and_logs)
                self.prune_thread.start()
        # Start the historical run failure thread
        if self.fail_historical_runs:
            if self.fail_hist_thread is None or not self.fail_hist_thread.is_alive():
                self.fail_hist_thread = threading.Thread(target=self._fail_historical)
                self.fail_hist_thread.start()
        # Start the task refreshing thread
        if self.refresh_tasks_thread is None or not self.refresh_tasks_thread.is_alive():
            self.refresh_tasks_thread = threading.Thread(target=self._refresh_tasks)
            self.refresh_tasks_thread.start()
        return self.thread

    def stop(self):
        scheduler_log.add_entry(
            actor='scheduler', category='status', text='Stopping', json={}
        )
        self.running_state = RunningState.stopped
        if self.thread is not None:
            self.thread.join()

    def pause(self):
        scheduler_log.add_entry(
            actor='scheduler', category='status', text='Pausing', json={}
        )
        self.running_state

    def _prune_runs_and_logs(self):
        while self.running_state != RunningState.stopped:
            time.sleep(self.prune_interval)
            # Loop while we're not stopped, but only do stuff if we're running
            if self.running_state != RunningState.running:
                continue
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

    def _fail_historical(self):
        """
        This will fail:
        - Runs that were scheduled or started but didn't finish within the time
        - Runs that have been inactive for over 5 minutes
        """
        while self.running_state != RunningState.stopped:
            # If the scheduler is being started in the same environment as the
            # task runner, then we need to wait for the task runner to start
            # and load the tasks before we can check for historical runs
            # otherwise we won't have any tasks to check
            time.sleep(60)
            # Loop while we're not stopped, but only do stuff if we're running
            if self.running_state != RunningState.running:
                continue
            if not self.fail_historical_runs or self.fail_historical_age is None:
                continue
            for task in self.all_tasks:
                open_runs = task.get_running_runs() + task.get_queued_runs()
                historical_count = 0
                for run in open_runs:
                    run_age = dt.now() - run.scheduled_time
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
                            if run.last_active < dt.now() - td(minutes=5):
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
            # Sleep after each check so on first load it does a check and
            # flush of all 'old' runs
            time.sleep(self.fail_historical_interval)

    def _refresh_tasks(self):
        while self.running_state != RunningState.stopped:
            time.sleep(self.task_refresh_interval)
            # Loop while we're not stopped, but only do stuff if we're running
            if self.running_state == RunningState.running:
                self.all_tasks = TaskItem.get_all()
                scheduler_log.add_entry(
                    actor='scheduler', category='refresh_tasks',
                    text='Refreshing tasks',
                    json={'task_count': len(self.all_tasks)}
                )

    def _process_schedules(self):
        while self.running_state != RunningState.stopped:
            time.sleep(15)
            # log that we're processing schedules and log which tasks
            scheduler_log.add_entry(
                actor='main_loop', category='status', text='Processing schedules',
                json={
                    'task_count': len(self.all_tasks),
                    'task_names': ', '.join([task.task_idk for task in self.all_tasks])
                }
            )
            self.update_active()
            # Loop while we're not stopped, but only do stuff if we're running
            if self.running_state != RunningState.running:
                continue

            if len(self.all_tasks) == 0:
                self.all_tasks = TaskItem.get_all()

            for task in self.all_tasks:
                # Only check enabled tasks (e.g. no disabled/inactive tasks)
                if task.status != 'enabled':
                    continue
                for schedule in task.schedule_sets:
                    is_due, last_run = task.is_run_due_with_last(schedule)
                    if is_due:
                        # TODO Check for old queued/running runs and set them to failed
                        # No longer failing runs that are queued and relying on
                        # the historical run failure to do the work
                        if self.disable_stale_tasks and last_run is not None:
                            # If the task hasn't been active since the last run,
                            # then it's stale and should be disabled.
                            # Tasks should be checked every 5s, and runs at most frequent, every 1 minute
                            # so a task should have been active many times since the last run
                            if task.last_active < min(last_run.scheduled_time, dt.now() - td(minutes=5)):
                                task.set_status('inactive', 'Task has been inactive since last scheduled run')
                                Producer().send_message(
                                    channel=MqueueChannels.inactive_task,
                                    message=MqueueChannels.inactive_task.message_type(
                                        scheduler_id=self.scheduler_idk,
                                        task_id=task.task_idk
                                    )
                                )
                                continue
                        # print('Run due for task:', task.task_idk)
                        run = task.schedule_run(schedule)
                        if run is None:
                            raise Exception('Failed to create run')