from __future__ import annotations

import copy
import json
from abc import ABC
from dataclasses import dataclass
from datetime import datetime as dt
from datetime import timedelta as td
from enum import Enum
from typing import Callable, Literal
from uuid import uuid4

from croniter import croniter
from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.postgresql import insert, JSON as PG_JSON
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import text as sql

from orcha import current_time
from orcha.core import monitors
from orcha.core.monitors import AlertBase, AlertOutputType, MonitorBase
from orcha.utils import get_config_keys
from orcha.utils.log import LogManager
from orcha.utils.mqueue import Channel, Message, Producer
from orcha.utils.sqlalchemy import (
    get_latest_versions,
    postgres_scaffold,
    sqlalchemy_build,
)

print('Loading:',__name__)

_tasks_log = LogManager('tasks')

class VersionMismatchException(Exception):
    pass

class MqueueChannels():
    """
    This class is used to define the channels for the mqueue
    that the monitors and alerts will use.
    """
    class _RunFailedMessage:
        def __init__(self, task_id: str, run_id: str):
            self.task_id = task_id
            self.run_id = run_id

        def to_json(self) -> str:
            return json.dumps({
                "task_id": self.task_id,
                "run_id": self.run_id
            })

        @classmethod
        def from_json(cls, json_str: str):
            data = json.loads(json_str)
            return cls(task_id=data["task_id"], run_id=data["run_id"])

    run_failed = Channel(
        name='run_failed',
        message_type=_RunFailedMessage
    )


is_initialised = False
skip_initialisation_check = False
"""
Debug flag to skip the initialisation check for adding tasks
without a task runner in place.
"""

Base: DeclarativeMeta
engine: Engine
s_maker: sessionmaker[Session]

_register_task_with_runner: Callable | None = None

"""
===================================================================
 Initialisation functions
===================================================================
"""

def confirm_initialised():
    """
    Guard function to ensure that orcha has been initialised
    """
    if skip_initialisation_check:
        return
    if not is_initialised:
        raise RuntimeError('orcha not initialised. Call orcha.core.initialise() first')

def _setup_sqlalchemy(
        orcha_user: str, orcha_pass: str,
        orcha_server: str, orcha_db: str,
        orcha_schema: str, application_name: str
    ):
    global is_initialised, Base, engine, s_maker, TaskRecord, RunRecord
    is_initialised = True
    Base, engine, s_maker = postgres_scaffold(
        user=orcha_user,
        passwd=orcha_pass,
        server=orcha_server,
        db=orcha_db,
        schema=orcha_schema,
        application_name=application_name
    )
    class TaskRecord(Base):
        __tablename__ = 'tasks'

        task_idk = Column(String, primary_key=True)
        version = Column(DateTime(timezone=False), primary_key=True)
        task_metadata = Column(PG_JSON)
        task_tags = Column(PG_JSON)
        name = Column(String)
        description = Column(String)
        schedule_sets = Column(PG_JSON)
        thread_group = Column(String)
        last_active = Column(DateTime(timezone=False))
        status = Column(String)
        notes = Column(String)
        task_config = Column(PG_JSON)

    class RunRecord(Base):
        __tablename__ = 'runs'

        update_timestamp = Column(DateTime(timezone=False))
        run_idk = Column(String, primary_key=True)
        task_idf = Column(String)
        set_idf = Column(String)
        run_type = Column(String)
        created_time = Column(DateTime(timezone=False))
        created_by = Column(String)
        scheduled_time = Column(DateTime(timezone=False))
        start_time = Column(DateTime(timezone=False))
        end_time = Column(DateTime(timezone=False))
        last_active = Column(DateTime(timezone=False))
        config = Column(PG_JSON)
        progress = Column(String)
        status = Column(String)
        output = Column(PG_JSON)


    sqlalchemy_build(Base, engine, orcha_schema)

    # Critical index for the performace of fetching runs
    with s_maker.begin() as tx:
        tx.execute(sql('''
            --DROP INDEX IF EXISTS orcha.idx_orcha_runs_task_scheduled;
            CREATE INDEX IF NOT EXISTS idx_orcha_runs_task_scheduled
            ON orcha.runs (task_idf, scheduled_time, run_type);

            --DROP INDEX IF EXISTS orcha.idx_orcha_runs_task_set_scheduled;
            CREATE INDEX IF NOT EXISTS idx_orcha_runs_task_set_scheduled
            ON orcha.runs (task_idf, scheduled_time, set_idf, run_type);

            --DROP INDEX IF EXISTS orcha.idx_orcha_runs_taskidf_status_progress;
            CREATE INDEX IF NOT EXISTS idx_orcha_runs_taskidf_status_progress
            ON orcha.runs (task_idf, status, progress);
        '''))

"""
===================================================================
 Task Item classes and definitions
===================================================================
"""


TaskStatus = Literal['enabled', 'disabled', 'error', 'inactive', 'deleted']


@dataclass
class TriggerConfig():
    task: TaskItem
    schedule: ScheduleSet | None
    pass_config: bool = True


@dataclass
class ScheduleSet():
    set_idk: str | None
    cron_schedule: str
    config: dict
    trigger_config: TriggerConfig | None = None

    def __init__(
            self,
            cron_schedule: str,
            config: dict,
            trigger_config: TriggerConfig | None = None
        ) -> None:
        """
        Creates a schedule set for a task with a cron schedule and config.
        set_idk is generated automatically when the schedule set is added to
        a task which allows the same cron schedule to be used on multiple tasks.
        #### Parameters:
        - cron_schedule: The cron schedule for the task
        - config: The config for the task for this schedule
        - trigger_task: The task (and schedule set) to be triggered on successful completion
        of this schedule run.
        """
        self.set_idk = None
        self.cron_schedule = cron_schedule
        self.config = config
        self.trigger_config = trigger_config

    @staticmethod
    def list_to_dict(schedule_sets: list[ScheduleSet]) -> list[dict]:
        """
        Converts a list of ScheduleSet instances to a list of dictionaries.
        Typically used when saving to the database.
        """
        return [x.to_dict() for x in schedule_sets]

    @staticmethod
    def create_with_key(
            set_idk: str,
            cron_schedule: str,
            config: dict,
            trigger_config: TriggerConfig | None = None
        ) -> ScheduleSet:
        """
        Creates a schedule set for a task with a cron schedule and config.
        set_idk is generated automatically when the schedule set is added to
        a task which allows the same cron schedule to be used on multiple tasks.
        """
        s_set = ScheduleSet(
            cron_schedule=cron_schedule,
            config=config,
            trigger_config=trigger_config
        )
        s_set.set_idk = set_idk
        return s_set

    @classmethod
    def from_json(cls, json_str: str) -> ScheduleSet:
        """
        Creates a schedule set from a json string, commonly used
        when loading from the database.
        """
        data = json.loads(json_str)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict) -> ScheduleSet:
        """
        Creates a schedule set from a dictionary, commonly used
        when loading from the database.
        """
        s_set = cls.create_with_key(
            set_idk=data['set_idk'],
            cron_schedule=data['cron_schedule'],
            config=data['config'],
        )
        if data.get('trigger_task', None) is None:
            s_set.trigger_config = None
        else:
            task_item = TaskItem.get(data['trigger_task']['task_id'])
            if task_item is None:
                raise Exception('Task not found for trigger task')
            # s_set.trigger_task = (
            #     task_item,
            #     task_item.get_schedule_from_id(data['trigger_task']['set_id'])
            # )
            s_set.trigger_config = TriggerConfig(
                task=task_item,
                schedule=task_item.get_schedule_from_id(data['trigger_task']['set_id']),
                pass_config=data['trigger_task'].get('pass_config', True)
            )
        return s_set

    def to_dict(self):
        """
        Converts the schedule set to a dictionary, commonly used
        when saving to the database.
        """
        if not self.trigger_config:
            trigger_task_json = None
        else:
            set_id = None
            if self.trigger_config.schedule is not None:
                set_id = self.trigger_config.schedule.set_idk
            trigger_task_json = {
                'task_id': self.trigger_config.task.task_idk,
                'set_id': set_id,
                'pass_config': self.trigger_config.pass_config
            }
        return {
            'set_idk': self.set_idk,
            'cron_schedule': self.cron_schedule,
            'config': self.config,
            'trigger_task': trigger_task_json
        }


class TaskItem():
    """
    The TaskItem class is used to manage tasks in Orcha. This class
    provides functions to create, update, delete and manage tasks
    and their schedules. It also provides functions to get runs for
    the task and schedule runs.
    ### Note: Instanciating this class directly will not create a new task
    in the database. Use the create function to create a new task.
    ### Attributes:
    - task_id: The unique id for the task. Used as a PK in the database.
    - version: The version of the task
    - task_metadata: Additional metadata for the task
    - task_tags: A list of tags for the task
    - name: The name of the task
    - description: A description of the task
    - schedule_sets: A list of ScheduleSet instances for the task
    - thread_group: The thread group for the task
    - last_active: The last time the task was active
    - status: The status of the task
    - notes: Additional notes for the task
    - task_monitors: A list of monitors to raise alerts for the task
    """
    task_idk: str
    version: dt
    task_metadata: dict
    task_tags: list[str]
    name: str
    description: str
    schedule_sets: list[ScheduleSet]
    thread_group: str
    last_active: dt
    status: TaskStatus
    notes: str | None = None
    task_monitors: list[TaskMonitorBase] = []

    def __init__(
            self, task_idk: str, version: dt, task_metadata: dict, name: str,
            description: str, schedule_sets: list[ScheduleSet] | list[dict],
            thread_group: str, last_active: dt, status: TaskStatus,
            task_tags: list[str], notes: str | None = None,
            task_config: dict = {}
        ) -> None:

        confirm_initialised()

        # Legacy checks for old task items from the database
        # to make sure all fields are correctly set
        task_tags = task_tags if task_tags is not None else []

        # If the schedule sets are passed as a dict, most likely from
        # the database, then convert them to a list of ScheduleSet objects
        sets = []
        for schedule_set in schedule_sets:
            if isinstance(schedule_set, dict):
                sets.append(ScheduleSet.from_dict(schedule_set))
            else:
                sets.append(schedule_set)
        self.task_idk = task_idk
        self.version = version
        self.task_metadata = task_metadata
        self.task_tags = task_tags
        self.name = name
        self.description = description
        self.schedule_sets = sets
        self.thread_group = thread_group
        self.last_active = last_active
        self.status = status
        self.notes = notes
        self.task_config = task_config


    @staticmethod
    def get_all() -> list[TaskItem]:
        """
        Returns all tasks in the database as a list of TaskItem instances.
        """
        confirm_initialised()
        data = get_latest_versions(
            s_maker=s_maker,
            table='orcha.tasks',
            key_columns=['task_idk'],
            version_column='version',
            select_columns='*'
        )
        return [TaskItem(**(x._asdict())) for x in data]

    @staticmethod
    def get(task_idk: str) -> TaskItem | None:
        """
        Returns a task by its task_idk. If no task is found then None is returned.
        """
        confirm_initialised()
        data = get_latest_versions(
            s_maker=s_maker,
            table='orcha.tasks',
            key_columns=['task_idk'],
            version_column='version',
            select_columns='*',
            match_pairs=[('task_idk', '=', task_idk)]
        )
        tasks = [TaskItem(**(x._asdict())) for x in data]
        if len(tasks) == 0:
            return None
        if len(tasks) > 1:
            raise Exception('Multiple tasks found with same idk')
        return tasks[0]

    @classmethod
    def create(
            cls, task_idk: str, name: str, description: str,
            schedule_sets: list[ScheduleSet],
            task_function: Callable[[TaskItem | None, RunItem | None, dict], None],
            thread_group: str = 'base_thread',
            task_metadata: dict = {},
            task_tags: list[str] = [],
            register_with_runner: bool = True,
            task_monitors: list[TaskMonitorBase] = []
        ):
        """
        Creates a new task with the given parameters. This will create a new
        task in the database and register it with the task runner if required.
        #### Parameters:
        - task_id: The unique id used to identify this task. This is used
            to identify the task in the database and in the task runner and
            for updates, enabling/disabling and deleting
        - name: The name of the task
        - description: A description of the task
        - schedule_sets: A list of ScheduleSet objects that define the
            schedules for the task for when it should run.
        - thread_group: The thread group to use for the task. All tasks in the
            same thread group will be run in the same thread.
        - status: The status of the task. This can be used to disable a task when
            no longer required. Tasks must be explicitly disabled to prevent
            the scheduler from queuing runs for them.
        - task_metadata: Additional metadata for the task, no specific requirements
            are placed on this data, however is used by Orcha UI for workspaces.
        - task_tags: A list of tags for the task. These can be used to group tasks
            together for filtering and searching (used by Orcha UI)
        - register_with_runner: If True, the task will be registered with the runner
        - task_monitors: A list of TaskMonitorBase objects that define how to monitor
            the task and raise alerts if required.
        """

        confirm_initialised()

        version = current_time()
        current_task = TaskItem.get(task_idk)
        new_s_sets: list[ScheduleSet] = []
        for schedule in schedule_sets:
            # set the set_idk as task_id+cron_schedule
            new_s_sets.append(ScheduleSet.create_with_key(
                set_idk=f'{task_idk}_{schedule.cron_schedule}',
                cron_schedule=schedule.cron_schedule,
                config=schedule.config,
                trigger_config=schedule.trigger_config
            ))

        update_needed = False
        if current_task is None:
            update_needed = True
        elif (
            current_task.task_metadata != task_metadata or
            current_task.task_tags != task_tags or
            current_task.name != name or
            current_task.description != description or
            current_task.schedule_sets != new_s_sets or
            current_task.thread_group != thread_group
        ):
            update_needed = True

        # Only re-enable inactive tasks, not disabled ones
        task_status = 'enabled'
        if current_task is not None:
            if current_task.status == 'inactive':
                task_status = 'enabled'
            else:
                task_status = current_task.status

        # Create and register the task with the task runner
        # before we check if it needs updating otherwise
        # we'll not register the task
        task = TaskItem(
            task_idk=task_idk,
            version=version,
            task_metadata=task_metadata,
            task_tags=task_tags,
            name=name,
            description=description,
            schedule_sets=new_s_sets,
            thread_group=thread_group,
            last_active=version,
            status=task_status,
            notes=None
        )

        task.task_function = task_function # type: ignore
        task.task_config = get_config_keys(task_function)

        for monitor in task_monitors:
            monitor.add_task(task)

        if register_with_runner and not skip_initialisation_check:
            if _register_task_with_runner is None:
                raise Exception('No task runner registered')
            _register_task_with_runner(task)

        if not update_needed and current_task is not None:
            task.version = current_task.version
            return task
        else:
            task._update_db()
            return task

    def delete_from_db(self) -> None:
        """
        Deletes the task from the database. This will not delete the task
        from the task runner if it is registered.
        """
        # Only delete tasks that aren't enabled
        if self.status == 'enabled':
            raise Exception('Cannot delete enabled task')
        with s_maker.begin() as session:
            # Delete the task and all runs
            session.execute(sql('''
                DELETE FROM orcha.tasks
                WHERE task_idk = :task_idk;
                DELETE FROM orcha.runs
                WHERE task_idf = :task_idk;
            '''), {'task_idk': self.task_idk})

    def _update_db(self) -> None:
        """
        Internal function to update the task in the database.
        Note: Either updates the current version or creates a new version
        if the version has been updated elsewhere.
        """
        with s_maker.begin() as session:
            task_record = {
                'task_idk': self.task_idk,
                'version': self.version,
                'task_metadata': self.task_metadata,
                'task_tags': self.task_tags,
                'name': self.name,
                'description': self.description,
                'schedule_sets': ScheduleSet.list_to_dict(self.schedule_sets),
                'thread_group': self.thread_group,
                'last_active': self.last_active,
                'status': self.status,
                'notes': self.notes,
                'task_config': self.task_config
            }

            insert_stmt = insert(TaskRecord).values(task_record)
            update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['task_idk', 'version'],
                set_=task_record
            )

            session.execute(update_stmt)

    def set_status(self, status: TaskStatus, notes: str) -> None:
        """
        Used to enable/disable a task. This is used to prevent the scheduler
        from queuing runs for the task.
        """
        self.status = status
        self.notes = notes
        # Toggling status will create a new version
        self.version = current_time()
        self._update_db()

    def set_enabled(self, notes: str) -> None:
        """
        Sets the task status to enabled. This is used to re-enable a task
        from any status.
        """
        if self.status == 'enabled':
            return
        self.set_status('enabled', notes)

    def update_active(self) -> None:
        """
        Used to indicate to the scheduler the last time the task was active.
        Old tasks that have not been active for a while will be automatically
        disabled by the scheduler. This will reactivate any task that has been
        disabled due to inactivity by the scheduler.
        """
        if self.status == 'inactive':
            self.set_enabled('update_active reactivated task')
        self.last_active = current_time()
        self._update_db()

    def get_schedule_set(self, set_idk: str) -> ScheduleSet | None:
        """
        Gets a schedule set by its set_idk. If no schedule set is found then
        None is returned.
        """
        for schedule in self.schedule_sets:
            if schedule.set_idk == set_idk:
                return schedule
        return None

    def get_schedule_from_id(self, set_idk: str) -> ScheduleSet | None:
        """
        Gets a schedule set by its set_idk. If no schedule set is found then
        None is returned.
        """
        for schedule in self.schedule_sets:
            if schedule.set_idk == set_idk:
                return schedule
        return None

    def get_last_scheduled(self, schedule: ScheduleSet) -> dt:
        """
        Returns the last time the task was scheduled to run for the
        particular schedule set.
        """
        cron_schedule = schedule.cron_schedule
        return croniter(cron_schedule, current_time()).get_prev(dt)

    def get_time_between_runs(self, schedule: ScheduleSet) -> td:
        """
        Returns the time between the last two scheduled runs for the
        particular schedule set. This is used to calculate the time
        between runs for the task.
        """
        cron = croniter(schedule.cron_schedule, current_time())
        next_run_time_1 = cron.get_next(dt)
        next_run_time_2 = cron.get_next(dt)
        time_delta = next_run_time_2 - next_run_time_1
        return time_delta

    # TODO - pulling all runs from the db is not great.
    # this needs be bounded by time/something
    # def get_runs(self) -> list[RunItem]:
    #     return RunItem.get_all(
    #         task_id=self.task_idk,
    #         since=dt(2023, 1, 1)
    #     )

    def get_last_run(self, schedule: ScheduleSet | None) -> RunItem | None:
        """
        Gets the latest run for the task. If no schedule is provided
        then the latest run from all schedules is returned.
        """
        return RunItem.get_latest(task=self, schedule=schedule)

    def get_latest_runs(self, schedule: ScheduleSet | None, count: int) -> list[RunItem]:
        """
        Gets the latest count runs for the task. If no schedule is provided
        then runs from all schedules are returned.
        Ordered by scheduled time descending.
        """
        runs = RunItem.get_all(task=self, since=dt.min, schedule=schedule)
        runs = sorted(runs, key=lambda x: x.scheduled_time, reverse=True)
        return runs[:count]

    def get_next_scheduled_time(self, schedule: ScheduleSet | None = None) -> dt | None:
        """
        Returns the next scheduled time for the task. If no schedule is
        provided then the first schedule set is used.
        #### Args:
        - schedule: The schedule set to get the next scheduled time for. If None
            then the first schedule set is used.
        #### Returns:
        - The next scheduled time for the task or None if no schedule sets are available.
        """
        if schedule is None:
            if len(self.schedule_sets) == 0:
                return None
            schedule = self.schedule_sets[0]
        cron_schedule = schedule.cron_schedule
        return croniter(cron_schedule, current_time()).get_next(dt)

    def is_run_due(self, schedule: ScheduleSet):
        """
        Returns if a run is due for the particular schedule set.
        """
        is_due, _ = self.is_run_due_with_last(schedule)
        return is_due

    def is_run_due_with_last(self, schedule: ScheduleSet) -> tuple[bool, RunItem | None]:
        """
        Returns if a run is due for the particular schedule set and
        the last run instance to save calling get_last_run twice
        Returns a tuple of (is_due, last_run)
        """
        last_run = RunItem.get_latest(
            task=self, schedule=schedule, run_type='scheduled'
        )
        if last_run is None:
            return True, last_run
        return last_run.scheduled_time < self.get_last_scheduled(schedule), last_run

    def schedule_run(
            self,
            schedule_by_id: str,
            schedule: ScheduleSet,
            force_run: bool = False
        ) -> RunItem | None:
        """
        Schedules a run for the task and schedule set and returns the run instance.
        This creates a run regardless of whether a run is due or not.
        #### Parameters:
        - schedule: The schedule set to schedule the run for
        - force_run: If True, the run will be created even if the task is not enabled
        """
        if self.status == 'enabled' or force_run:
            return RunItem.create(
                task=self,
                run_type='scheduled',
                scheduled_time=self.get_last_scheduled(schedule),
                schedule=schedule,
                created_by=schedule_by_id
            )
        else:
            return None

    def trigger_run(
            self,
            trigger_task: TaskItem,
            scheduled_time: dt,
            force_run: bool = False,
            run_config: dict | None = None
        ) -> RunItem | None:
        """
        Triggers a run for the task and schedule set and returns the run instance.
        This creates a run regardless of whether a run is due or not.
        #### Parameters:
        - schedule: The schedule set to trigger the run for
        - trigger_task: The task that triggered this run
        - scheduled_time: The time the run was scheduled to run
        - force_run: If True, the run will be created even if the task is not enabled
        - run_config: If provided, this config will override any keys already
            set in the schedule set config
        """
        if self.status == 'enabled' or force_run:
            run = RunItem.create(
                task=self,
                run_type='triggered',
                scheduled_time=scheduled_time,
                schedule=None,
                created_by=trigger_task.task_idk,
                config_override=run_config
            )
            return run
        else:
            return None

    def get_queued_runs(self) -> list[RunItem]:
        """
        Returns all queued runs for the task.
        This is an alias for RunItem.get_all_queued(task=<task_idk>)
        """
        return RunItem.get_all_queued(task=self)

    def get_running_runs(self) -> list[RunItem]:
        """
        Returns all running runs for the task.
        This is an alias for RunItem.get_running_runs(task=<task_idk>)
        """
        return RunItem.get_running_runs(task=self)

    def prune_runs(self, max_age: td | None) -> int:
        """
        Prunes runs that are older than max_age and keeps the most recent
        max_count runs. This is useful for keeping the database size down.
        Returns the number of runs deleted.
        """
        if max_age is None:
            return 0
        with s_maker.begin() as session:
            query = '''
                WITH deleted AS (
                    DELETE
                    FROM
                        orcha.runs
                    WHERE
                        task_idf = :task_idf
                        AND scheduled_time < :date_cutoff
                    RETURNING *
                )
                SELECT COUNT(*) AS "del_count" FROM deleted
            '''

            deleted_rows = session.execute(sql(query), {
                'task_idf': self.task_idk,
                'date_cutoff': current_time() - max_age
            }).all()

            if len(deleted_rows) == 0:
                raise Exception('Prune runs failed')
            if not hasattr(deleted_rows[0], 'del_count'):
                raise Exception('Prune runs failed')
            return deleted_rows[0].del_count

    def task_function(self, task: TaskItem | None, run: RunItem | None, config: dict) -> None:
        """
        The Orcha task runner will pass the current run instance
        to this function which can be used to update the run status
        as required, however the function can be manually called with
        no run instance if required (e.g. for testing)
        args:
            task: The task that is the owner of this run
            run: The current run instance
            config: The task config for the current run from the schedule set
        """
        raise NotImplementedError(f'task_id {self.task_idk} does not implement task_function')


"""
===================================================================
 Run Item classes and definitions
===================================================================
"""


RunStatus = Literal['unstarted', 'pending', 'success', 'warn', 'failed', 'cancelled']
class RunStatusEnum(Enum):
    """
    The Enum for the status of a run primarily as an auto-complete helper
    as a replacement for the string literals.
    """
    unstarted = 'unstarted'
    pending = 'pending'
    success = 'success'
    warn = 'warn'
    failed = 'failed'
    cancelled = 'cancelled'

RunProgress = Literal['queued', 'running', 'complete']
class RunProgressEnum(Enum):
    """
    The Enum for the progress of a run primarily as an auto-complete helper
    as a replacement for the string literals.
    """
    queued = 'queued'
    running = 'running'
    complete = 'complete'

RunType = Literal['scheduled', 'manual', 'retry', 'triggered']
"""
The types of runs that can be created.
- scheduled: A run that is created by the scheduler
- manual: A run that is created manually as a 'one-off'
- retry: A run that is created as a retry of a failed run
- triggered: A run that is triggered on completion of another task
"""

@dataclass
class RunItem():
    """
        This class manages the run instances for tasks and write data back
        to the database. It also provides functions to get run instances. An
        instance of this class is a representation of a run in the database.
        ### Note: Instanciating this class directly will not create a new run
        in the database. Use the create function to create a new run.
        ### Attributes:
        - _task: The task instance that this run is associated with
        - run_idk: The unique id for this run
        - task_idf: The task id for this run
        - set_idf: The schedule set id for this run
        - run_type: The type of run (scheduled, manual, retry)
        - created_time: The time the run was created
        - created_by: the user/schedule/task that created the run
        - scheduled_time: The time the run was scheduled to run
        - start_time: The time the run started
        - end_time: The time the run ended
        - last_active: The last time the run was active
        - config: The config for the run from the schedule set
        - status: The status of the run (success, warn, failed, cancelled)
        - progress: The progress of the run (queued, running, complete)
        - output: The output of the run which includes all outputs from modules and the task function
    """
    _task: TaskItem
    update_timestamp: dt
    run_idk: str
    task_idf: str
    set_idf: str | None
    run_type: str
    created_time: dt
    created_by: str
    scheduled_time: dt
    start_time: dt | None
    end_time: dt | None
    last_active: dt | None
    config: dict
    status: RunStatus
    progress: RunProgress
    output: dict | None = None

    @staticmethod
    def _task_id_populate(task: str | TaskItem) -> TaskItem:
        """
        Internal function. Populates the unprovided task_id or task used
        by various functions.
        """
        if isinstance(task, TaskItem):
            return task

        cur_task = TaskItem.get(task)
        if cur_task is None:
            raise Exception(f'Internal error: Cannot populate task_id {task}')
        return  cur_task

    @staticmethod
    def _from_record(record: RunRecord, task: TaskItem | None) -> RunItem:
        task_idf = None
        if hasattr(record, 'task_idf'):
            task_idf = record.task_idf
        if task_idf is None:
            raise Exception('Task id not found for run')
        if task and task.task_idk != task_idf:
            raise Exception('Task id from record does not match provided task')
        if task is None:
            task = TaskItem.get(task_idf) # type: ignore
        if task is None:
            raise Exception('Task not found for run')
        return RunItem(
            _task = task,
            update_timestamp = record.update_timestamp, # type: ignore
            run_idk = record.run_idk, # type: ignore
            task_idf = record.task_idf, # type: ignore
            set_idf = record.set_idf, # type: ignore
            run_type = record.run_type, # type: ignore
            created_time = record.created_time, # type: ignore
            created_by = record.created_by, # type: ignore
            scheduled_time = record.scheduled_time, # type: ignore
            start_time = record.start_time, # type: ignore
            end_time = record.end_time, # type: ignore
            last_active = record.last_active, # type: ignore
            config = record.config, # type: ignore
            status = record.status, # type: ignore
            progress = record.progress, # type: ignore
            output = record.output # type: ignore
        )

    @staticmethod
    def create(
            task: TaskItem, run_type: RunType,
            schedule: ScheduleSet | None, scheduled_time: dt,
            created_by: str, config_override: dict | None = None
        ) -> RunItem:
        """
        Creates a new run instance for a task with a new uuid and
        'new run' defaults in the database. This is a separate function to the
        __init__ to keep creating database entries separate from instanciating.
        Manual and triggered runs can be created without a schedule set.
        """
        confirm_initialised()

        sset_id = None
        run_config = {}

        # Scheduled runs need to handle the schedule set whereas
        # manual and triggered runs have no schedule set and use the
        # config_override only
        if run_type == 'scheduled' or run_type == 'retry':
            if schedule is None:
                raise Exception('Scheduled run requires a schedule set')
            sset_id = schedule.set_idk
            # use the override config if provided
            run_config = config_override or schedule.config
        elif run_type == 'manual' or run_type == 'triggered':
            # if we have a schedule for these runs then use the config or override
            if schedule is not None:
                sset_id = schedule.set_idk
                run_config = config_override or schedule.config
            elif config_override is not None:
                run_config = config_override
            else:
                raise Exception(
                    'Manual/triggered run requires a config override or schedule set'
                )
        else:
            raise Exception('Invalid run type')


        item = RunItem(
            _task = task,
            update_timestamp = current_time(),
            run_idk = str(uuid4()),
            task_idf = task.task_idk,
            set_idf = sset_id,
            run_type = run_type,
            created_time = current_time(),
            created_by = created_by,
            scheduled_time = scheduled_time,
            start_time = None,
            end_time = None,
            last_active = None,
            config = run_config,
            status = 'unstarted',
            progress = 'queued',
            output = None
        )

        item._update_db(ignore_updated_check=True)
        return item

    @staticmethod
    def get_all(
            task: str | TaskItem,
            since: dt,
            max_count: int | None = None,
            schedule: ScheduleSet | None = None,
            run_type: RunType | None = None,
            status: RunStatus | None = None,
            progress: RunProgress | None = None
        ) -> list[RunItem]:
        """
        Gets all runs for a task since a particular time (inclusive)
        for a particular schedule set (optional, None for all runs). Results
        are sorted by scheduled time descending.
        #### Parameters:
        - task: The task instance or task id for the task
        - since: The time to get runs since
        - max_count: The maximum number of runs to get
        - schedule: The schedule set to get the runs for, or None for all schedules
        - run_type: The type of run to get the runs for, or None for all types
        - status: The status of the runs to get, or None for all statuses
        - progress: The progress of the runs to get, or None for all progress
        #### Returns:
        - A list of RunItem instances for the task and schedule set
        """
        confirm_initialised()
        task = RunItem._task_id_populate(task)

        # make sure the schedule set is for this task
        task_schedule_sets = [x.set_idk for x in task.schedule_sets]
        if schedule is not None and schedule.set_idk not in task_schedule_sets:
            raise Exception('Schedule set not found for task')

        with s_maker.begin() as session:
            filter_sets = [
                RunRecord.task_idf == task.task_idk,
                RunRecord.scheduled_time >= since
            ]
            if status is not None:
                filter_sets.append(RunRecord.status == status)
            if progress is not None:
                filter_sets.append(RunRecord.progress == progress)
            if run_type is not None:
                filter_sets.append(RunRecord.run_type == run_type)
            if schedule is not None:
                if schedule.set_idk is None:
                    raise Exception('set_idk not set: cannot get runs for schedule set without id')
                filter_sets.append(RunRecord.set_idf == schedule.set_idk)
            records = session.query(RunRecord).filter(*filter_sets).order_by(
                RunRecord.scheduled_time.desc()
            ).limit(max_count).all()
            return [RunItem._from_record(r, task) for r in records]

    @staticmethod
    def get_all_queued(
            task: str | TaskItem,
            schedule: ScheduleSet | None = None,
        ) -> list[RunItem]:
        """
        Returns all runs that are queued (and unstarted) for a task and schedule set.
        """
        confirm_initialised()
        task = RunItem._task_id_populate(task)

        return RunItem.get_all(
            task=task,
            since=dt.min,
            schedule=schedule,
            status='unstarted',
            progress='queued'
        )

    @staticmethod
    def get_running_runs(
            task: str | TaskItem,
            schedule: ScheduleSet | None = None,
        ) -> list[RunItem]:
        """
        Returns all runs that are currently running for a task and schedule set.
        """
        confirm_initialised()
        task = RunItem._task_id_populate(task)

        return RunItem.get_all(
            task=task,
            since=dt.min,
            schedule=schedule,
            progress='running'
        )

    @staticmethod
    def get_latest(
            task: str | TaskItem,
            schedule: ScheduleSet | None = None,
            run_type: RunType | None = None
        ) -> RunItem | None:
        """
        Returns the latest run (scheduled time descending) for a task and schedule set.
        #### Parameters:
        - task: The task instance or task id for the task
        - schedule: The schedule set to get the latest run for, or None for all schedules
        - run_type: The type of run to get the latest for, or None for all types

        #### Returns:
        - The latest run for the task and schedule set, or None if no runs found
        """
        confirm_initialised()
        task = RunItem._task_id_populate(task)

        with s_maker.begin() as session:
            filter_sets = [
                RunRecord.task_idf == task.task_idk
            ]
            if run_type is not None:
                filter_sets.append(RunRecord.run_type == run_type)
            if schedule is not None:
                if schedule.set_idk is None:
                    raise Exception(
                        "set_idk not set: cannot get runs for schedule set without id"
                    )
                filter_sets.append(RunRecord.set_idf == schedule.set_idk)
            record = (
                session.query(RunRecord)
                .filter(*filter_sets)
                .order_by(RunRecord.scheduled_time.desc())
                .first()
            )
            if record is None:
                return None
            return RunItem._from_record(record, task)

    @staticmethod
    def get(run_id: str, task: TaskItem | None = None) -> RunItem | None:
        """
        Returns a run by its run_id. If no run is found then None is returned.
        """
        confirm_initialised()
        with s_maker.begin() as session:
            data = session.query(RunRecord).filter(
                RunRecord.run_idk == run_id).all()
            if len(data) == 0:
                return None
            if len(data) > 1:
                raise Exception('Multiple runs found with same idk')
            return RunItem._from_record(data[0], task)

    def reload(self):
        """
        Used to reload the run from the database to get the latest data.
        """
        db_data = RunItem.get(self.run_idk, task=self._task)
        if db_data is None:
            raise Exception('Run not found in database')
        self.__dict__.update(db_data.__dict__)

    def delete(self) -> None:
        """
        Deletes the run from the database.
        #### Note: Does not delete the instance, just the database entry.
        """
        with s_maker.begin() as session:
            session.execute(sql('''
                DELETE FROM orcha.runs
                WHERE run_idk = :run_idk
            '''), {'run_idk': self.run_idk})

    def _update_db(self, ignore_updated_check: bool = False):
        try:
            with s_maker.begin() as session:
                # TODO potential code for detecting concurrent updates
                #       and avoiding overwriting changes
                # update the run in the database if updated == updated
                # to prevent overwriting changes from other processes
                # and then update the updated time to the current time
                update_dt = current_time()
                updated_rows = session.execute(sql('''
                    INSERT INTO orcha.runs (
                        run_idk, update_timestamp, task_idf, set_idf, run_type,
                        created_time, created_by, scheduled_time, start_time, end_time,
                        last_active, config, status, progress, output
                    ) VALUES (
                        :run_idk, :update_timestamp, :task_idf, :set_idf, :run_type,
                        :created_time, :created_by, :scheduled_time, :start_time, :end_time,
                        :last_active, :config, :status, :progress, :output
                    )
                    ON CONFLICT (run_idk) DO UPDATE SET
                        update_timestamp = EXCLUDED.update_timestamp,
                        task_idf = EXCLUDED.task_idf,
                        set_idf = EXCLUDED.set_idf,
                        run_type = EXCLUDED.run_type,
                        created_time = EXCLUDED.created_time,
                        created_by = EXCLUDED.created_by,
                        scheduled_time = EXCLUDED.scheduled_time,
                        start_time = EXCLUDED.start_time,
                        end_time = EXCLUDED.end_time,
                        last_active = EXCLUDED.last_active,
                        config = EXCLUDED.config,
                        status = EXCLUDED.status,
                        progress = EXCLUDED.progress,
                        output = EXCLUDED.output
                    WHERE orcha.runs.update_timestamp = :last_updated OR :ignore_updated_check
                    RETURNING *
                '''), {
                    'update_timestamp': update_dt,
                    'last_updated': self.update_timestamp,
                    'run_idk': self.run_idk,
                    'task_idf': self._task.task_idk,
                    'set_idf': self.set_idf,
                    'run_type': self.run_type,
                    'created_time': self.created_time,
                    'created_by': self.created_by,
                    'scheduled_time': self.scheduled_time,
                    'start_time': self.start_time,
                    'end_time': self.end_time,
                    'last_active': self.last_active,
                    'config': json.dumps(self.config) if self.config is not None else None,
                    'status': self.status,
                    'progress': self.progress,
                    'output': json.dumps(self.output) if self.output is not None else None,
                    'ignore_updated_check': ignore_updated_check
                })

                if len(updated_rows.all()) == 0:
                    raise VersionMismatchException('Run update using mismatched versions')
                self.updated = update_dt
        except Exception as e:
            _tasks_log.add_entry(
                actor='run_item', category='database',
                text='error updating run in database',
                json={
                    'error': str(e),
                    'run_idk': self.run_idk,
                    'task_idk': self._task.task_idk,
                    'status': self.status,
                    'progress': self.progress,
                    'start_time': str(self.start_time),
                    'end_time': str(self.end_time),
                    'output': str(self.output)
                }
            )
            if isinstance(e, VersionMismatchException):
                raise e
            else:
                raise Exception(f'Error updating run in database: {e}') from e

    def update_active(self):
        """
        Updates the last active time for the run to the current time.
        """
        # We're directly updating the database here as all that is being
        # updated is the last_active time which is purely a change to this
        # one column and nothing else; don't need to reload the run as
        # we don't care if we 'roll back' a last_active time every now and then
        self.last_active = current_time()
        with s_maker.begin() as session:
            session.execute(sql('''
                UPDATE orcha.runs
                SET last_active = :last_active
                WHERE run_idk = :run_idk
            '''), {
                'last_active': self.last_active,
                'run_idk': self.run_idk
            })

    def update(
            self, status: RunStatus, progress: RunProgress,
            start_time: dt | None , end_time: dt | None,
            output: dict | None = None
        ):
        """
        Updates the run with the new status, progress, start time, end time and output.
        """
        # Keep initial values so we can figure out if we can recover
        # from a version mismatch
        version_status = self.status
        version_progress = self.progress
        version_start_time = self.start_time
        version_end_time = self.end_time
        version_output = self.output

        try_count = 0
        max_tries = 3
        while try_count < 3:
            try:
                # Updating 'self' inside the loop as if we fail, the reload()
                # will reset 'self' to the database values each time
                change_log = ''
                needs_update = False
                if changing_status := self.status != status:
                    needs_update = True
                    change_log += f' status: {self.status} -> {status}'
                    self.status = status
                if changing_progress := self.progress != progress:
                    needs_update = True
                    change_log += f' progress: {self.progress} -> {progress}'
                    self.progress = progress
                if changing_start_time := self.start_time != start_time:
                    needs_update = True
                    change_log += f' start_time: {self.start_time} -> {start_time}'
                    self.start_time = start_time
                if changing_end_time := self.end_time != end_time:
                    needs_update = True
                    change_log += f' end_time: {self.end_time} -> {end_time}'
                    self.end_time = end_time
                if changing_output := self.output != output:
                    needs_update = True
                    change_log += f' output: {str(self.output)} -> {str(output)}'
                    self.output = output

                if not needs_update:
                    return

                self._update_db()
                return
            except VersionMismatchException as e:
                # if we get a version mismatch, we need to reload the run
                # and try again
                self.reload()
                values_changed = ''
                if changing_status and version_status != self.status:
                    values_changed += f' status current: {version_status} -> {self.status}'
                if changing_progress and version_progress != self.progress:
                    values_changed += f' progress: {version_progress} -> {self.progress}'
                if changing_start_time and version_start_time != self.start_time:
                    values_changed += f' start_time current: {version_start_time} -> {self.start_time}'
                if changing_end_time and version_end_time != self.end_time:
                    values_changed += f' end_time current: {version_end_time} -> {self.end_time}'
                if changing_output and version_output != self.output:
                    values_changed += f' output current: {str(version_output)} -> {str(self.output)}'

                if values_changed == '':
                    try_count += 1

                if try_count >= max_tries or values_changed != '':
                    _tasks_log.add_entry(
                        actor='run_item', category='database',
                        text='error updating run in database',
                        json={
                            'error': 'Version mismatch',
                            'run_idk': self.run_idk,
                            'requested_changes': change_log,
                            'conflicting_changes': values_changed
                        }
                    )
                    raise e

    def set_status(
            self,
            status: RunStatus,
            output: dict | None = None,
            merge_output = True,
            send_alert = True,
            allow_backwards = False,
            raise_on_backwards = True
        ):
        """
        This sets the status of a run and neither updates the start or end time
        as these are set by the progress functions.
        #### Parameters:
        - status: The new status of the run
        - output: The output for the run
        - merge_output: If True, the output will be merged with any existing output
        - send_alert: If True, an alert will be sent for any status that has alerts
        - allow_backwards: If True, the status can be set to a lower status than
            the current status. Default is False.
        - raise_on_backwards: If True, an exception will be raised if the status
            is set to a lower status than the current status. Default is True.
            Note: This is only used if allow_backwards is False.

        """
        # See set_running for why we copy the output
        new_output = copy.deepcopy(output) if output else {}
        status_order = [
            RunStatusEnum.unstarted.value,
            RunStatusEnum.pending.value,
            RunStatusEnum.success.value,
            RunStatusEnum.warn.value,
            RunStatusEnum.failed.value,
            RunStatusEnum.cancelled.value
        ]
        # make sure we do the correct 'go backwards' behaviour
        backwards_change = status_order.index(status) < status_order.index(self.status)
        if not allow_backwards and backwards_change:
            if raise_on_backwards:
                raise Exception(f'Cannot set status to {status} from {self.status}')
            return

        if self.status == status:
            # if it's already set, we don't
            # want to update it again
            return

        if self.output is not None and merge_output:
            new_output.update(self.output)

        self.update(
            status = status,
            progress=self.progress,
            start_time = self.start_time,
            end_time = self.end_time,
            output = new_output
        )

        # Send the message after updating the database
        # otherwise the monitor won't see the updated status
        if send_alert:
            if status == RunStatusEnum.failed.value:
                Producer().send_message(
                    channel=MqueueChannels.run_failed,
                    message=MqueueChannels.run_failed.message_type(
                        task_id=self.task_idf,
                        run_id=self.run_idk
                    )
                )

    def set_progress(
            self,
            progress: RunProgress,
            output: dict | None = None,
            merge_output = True,
            zero_duration = False
        ):
        """
        This sets the progress of a run and updates the last active time.
        """
        self.reload()

        progress_order = [
            RunProgressEnum.queued.value,
            RunProgressEnum.running.value,
            RunProgressEnum.complete.value
        ]

        new_output = copy.deepcopy(output) if output else {}
        if self.progress == progress:
            # if it's already set, we don't
            # want to update it again
            return

        if progress_order.index(progress) < progress_order.index(self.progress):
            raise Exception(f'Cannot set progress to {progress} from {self.progress}')

        if self.output is not None and merge_output:
            new_output.update(self.output)

        if progress == RunProgressEnum.running.value:
            start_time = current_time()
            end_time = None
        elif progress == RunProgressEnum.complete.value:
            start_time = self.start_time
            if zero_duration:
                end_time = self.start_time
            else:
                end_time = current_time()
            start_time = self.start_time
        elif progress == RunProgressEnum.queued.value:
            start_time = None
            end_time = None

        self.update(
            status = self.status,
            progress = progress,
            start_time = start_time,
            end_time = end_time,
            output = new_output
        )


    def set_output(self, output: dict | None, merge = False):
        """
        Sets the output for the run. This will overwrite any existing output and
        and existing state (e.g. FAILED -> SUCCESS).
        If merge is set to True then the output will be merged with any
        existing output.
        """
        # Updating output can be problematic so we do a reload to make
        # sure we have the latest data and minimise the chance of having
        # a version conflict
        self.reload()
        if not merge:
            new_output = output
        else:
            if output is None:
                # merge + no output = do nothing
                return
            elif self.output is None:
                new_output = output
            else:
                new_output = copy.deepcopy(self.output)
                new_output.update(output)

        self.update(
            status = self.status,
            progress=self.progress,
            start_time = self.start_time,
            end_time = self.end_time,
            output = new_output
        )

"""
===================================================================
Task Monitor classes and definitions
===================================================================
"""

class TaskMonitorBase(MonitorBase, ABC):
    """
    The base abstract class to monitor a task.
    """

    def __init__(
            self,
            alert: AlertBase | Callable[[str], None],
            monitor_name: str,
            channel: Channel,
            check_function: Callable[[Channel, Message], None],
            tasks: set[TaskItem] = set()
        ):
        # Redefine the init method to include the tasks set
        # so that we can add tasks to the monitor in one hit
        # or the monitor can be added to the task on task creation
        super().__init__(alert, monitor_name, channel, check_function)
        self.tasks = tasks

    def add_task(self, task: TaskItem):
        """
        Adds a task to the monitor.
        """
        self.tasks.add(task)


class FailedRunsMonitor(TaskMonitorBase):
    """
    This class is used to monitor the status of a run and raise an
    alert if the run fails a certain number of times consecutively.
    Note: This monitor will not send alerts if the task has failed
    'too many times' to avoid spamming alerts. The recipient of the
    alert is expected to check the task.
    """

    def __init__(
            self,
            monitor_Name: str,
            alert: AlertBase,
            disable_after_count = 5,
        ):
        super().__init__(
            monitor_name=monitor_Name,
            alert=alert,
            channel=MqueueChannels.run_failed,
            check_function=self.check
        )
        self.alert = alert
        self.alert_on = RunStatusEnum.failed.value
        self.disable_after_count = disable_after_count

    def _run_to_ui_url(self, run: RunItem) -> str:
        if monitors.MONITOR_CONFIG and monitors.MONITOR_CONFIG.orcha_ui_base_url:
            href = f'{monitors.MONITOR_CONFIG.orcha_ui_base_url}/run_details?run_id={run.run_idk}'
            run_href = f'<a href="{href}">{run.run_idk}</a>'
            return run_href
        return run.run_idk

    def _task_to_ui_url(self, task: TaskItem) -> str:
        if monitors.MONITOR_CONFIG and monitors.MONITOR_CONFIG.orcha_ui_base_url:
            href = f'{monitors.MONITOR_CONFIG.orcha_ui_base_url}/task_details?task_id={task.task_idk}'
            task_href = f'<a href="{href}">{task.name}</a>'
            return task_href
        return task.name

    def check(self, channel: Channel, message: Message):
        """
        This method is used to monitor a run and will raise a failed alert.
        """
        if not isinstance(message, MqueueChannels.run_failed.message_type):
            raise Exception('Task monitor received invalid message type: ' + str(type(message)))

        # Don't do anything if the failed task isn't added to the monitor
        if message.task_id not in [x.task_idk for x in self.tasks]:
            return

        run = RunItem.get(message.run_id)
        task = TaskItem.get(message.task_id)

        if not task:
            raise Exception(f'Task ID ({message.task_id}) from message not found')
        if not run:
            raise Exception(f'Run ID ({message.run_id}) from message not found')
        # Check for 5 out of 7 runs to have failed,
        # this is to avoid spamming alerts if 4 fail, 1 succeeds
        # then 4 more fail, etc.
        runs = task.get_latest_runs(None, 10)
        fail_count = 0
        for r in runs:
            if r.status == self.alert_on:
                fail_count += 1

        if fail_count >= self.disable_after_count:
            task.set_status('error', f'Task set as failed after {fail_count} failed runs')
            if self.alert.output_type == AlertOutputType.HTML:
                message_string = f'''
                    <b>Task {self._task_to_ui_url(task)} set as failed after {fail_count} failured runs</b>
                    <br>
                    <br><b>Run ID</b>
                    <br>{self._run_to_ui_url(run)}
                '''
            else:
                message_string = f'''
                    Task {task.name} set as failed after {fail_count} failed runs.
                    Run ID: {run.run_idk}
                '''
        else:
            times_str = 'time' if fail_count == 1 else 'times'
            if self.alert.output_type == AlertOutputType.HTML:
                output_str = json.dumps(run.output, indent=4)
                output_str = output_str.replace('\\n', '<br>')
                message_string = f'''
                    <b>Task {self._task_to_ui_url(task)} has failed {fail_count} {times_str}</b>
                    <br>
                    <br><b>Run ID</b>
                    <br>{self._run_to_ui_url(run)}
                    <br>
                    <br><b>Run Output:</b>
                    <pre>{output_str}</pre>
                '''
            else:
                message_string = f'''
                    Task {task.name} has failed {fail_count} {times_str}
                    Run ID: {run.run_idk}
                    Run Output: {json.dumps(run.output)}
                '''

        self.alert.send_alert(message=message_string)