from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime as dt
from datetime import timedelta as td
from typing import Callable, Literal
from uuid import uuid4

from croniter import croniter
from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.postgresql import JSON as PG_JSON
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.sql import text as sql
from sqlalchemy.engine.mock import MockConnection
from sqlalchemy.orm import sessionmaker

from orcha.utils.sqlalchemy import (get, get_latest_versions, sqlalchemy_build,
                                    postgres_scaffold)


print('Loading dh:',__name__)

is_initialised = False

ORCHA_SCHEMA = 'orcha'

Base: DeclarativeMeta
engine: MockConnection
Session: sessionmaker

_register_task_with_runner: Callable | None = None

"""
===================================================================
 Initialisation functions
===================================================================
"""

def setup_sqlalchemy(
        orcha_user: str, orcha_pass: str,
        orcha_server: str, orcha_db: str
    ):
    global is_initialised, Base, engine, Session, TaskRecord, RunRecord
    is_initialised = True
    Base, engine, Session = postgres_scaffold(
        user=orcha_user,
        passwd=orcha_pass,
        server=orcha_server,
        db=orcha_db,
        schema=ORCHA_SCHEMA
    )
    class TaskRecord(Base):
        __tablename__ = 'tasks'

        task_idk = Column(String, primary_key=True)
        version = Column(DateTime(timezone=False), primary_key=True)
        task_metadata = Column(PG_JSON)
        name = Column(String)
        description = Column(String)
        schedule_sets = Column(PG_JSON)
        thread_group = Column(String)
        last_active = Column(DateTime(timezone=False))
        status = Column(String)
        notes = Column(String)

    class RunRecord(Base):
        __tablename__ = 'runs'

        run_idk = Column(String, primary_key=True)
        task_idf = Column(String)
        set_idf = Column(String)
        run_type = Column(String)
        scheduled_time = Column(DateTime(timezone=False))
        start_time = Column(DateTime(timezone=False))
        end_time = Column(DateTime(timezone=False))
        last_active = Column(DateTime(timezone=False))
        config = Column(PG_JSON)
        status = Column(String)
        output = Column(PG_JSON)


    sqlalchemy_build(Base, engine, ORCHA_SCHEMA)

    # Critical index for the performace of fetching runs
    with Session.begin() as tx:
        tx.execute(sql('''
            --DROP INDEX IF EXISTS orcha.idx_orcha_runs_task_scheduled;
            CREATE INDEX IF NOT EXISTS idx_orcha_runs_task_scheduled
            ON orcha.runs (task_idf, scheduled_time, run_type);

            --DROP INDEX IF EXISTS orcha.idx_orcha_runs_task_set_scheduled;
            CREATE INDEX IF NOT EXISTS idx_orcha_runs_task_set_scheduled
            ON orcha.runs (task_idf, scheduled_time, set_idf, run_type);
        '''))

"""
===================================================================
 Task Item classes and definitions
===================================================================
"""


class TaskStatus():
    ENABLED = 'enabled'
    DISABLED = 'disabled'
    DELETED = 'deleted'


class TaskType():
    ETL = 'etl'
    RETL = 'retl'
    QUALITY = 'quality'


@dataclass
class ScheduleSet():
    set_idk: str | None
    cron_schedule: str
    config: dict

    @staticmethod
    def list_to_dict(schedule_sets: list[ScheduleSet]) -> list[dict]:
        return [x.to_dict() for x in schedule_sets]

    def __init__(self, cron_schedule: str, config: dict) -> None:
        """
        Creates a schedule set for a task with a cron schedule and config.
        set_idk is generated automatically when the schedule set is added to
        a task which allows the same cron schedule to be used on multiple tasks.
        """
        self.set_idk = None
        self.cron_schedule = cron_schedule
        self.config = config

    @staticmethod
    def create_with_key(set_idk: str, cron_schedule: str, config: dict) -> ScheduleSet:
        """
        Creates a schedule set for a task with a cron schedule and config.
        set_idk is generated automatically when the schedule set is added to
        a task which allows the same cron schedule to be used on multiple tasks.
        """
        s_set = ScheduleSet(
            cron_schedule=cron_schedule,
            config=config
        )
        s_set.set_idk = set_idk
        return s_set

    def to_dict(self):
        return {
            'set_idk': self.set_idk,
            'cron_schedule': self.cron_schedule,
            'config': self.config
        }


class TaskItem():
    task_idk: str
    version: dt
    task_metadata: dict
    name: str
    description: str
    schedule_sets: list[ScheduleSet]
    thread_group: str
    last_active: dt
    status: str
    notes: str | None = None

    def __init__(
            self, task_idk: str, version: dt, task_metadata: dict, name: str,
            description: str, schedule_sets: list[ScheduleSet] | list[dict],
            thread_group: str, last_active: dt, status: str,
            notes: str | None = None
        ) -> None:
        # If the schedule sets are passed as a dict, most likely from
        # the database, then convert them to a list of ScheduleSet objects
        sets = []
        for schedule_set in schedule_sets:
            if type(schedule_set) == dict:
                sets.append(ScheduleSet.create_with_key(**schedule_set))
            else:
                sets.append(schedule_set)
        self.task_idk = task_idk
        self.version = version
        self.task_metadata = task_metadata
        self.name = name
        self.description = description
        self.schedule_sets = sets
        self.thread_group = thread_group
        self.last_active = last_active
        self.status = status
        self.notes = notes

    @staticmethod
    def get_all() -> list[TaskItem]:
        data = get_latest_versions(
            session=Session,
            table='orcha.tasks',
            key_columns=['task_idk'],
            version_column='version',
            select_columns='*'
        )
        return [TaskItem(**x) for x in data]

    @staticmethod
    def get(task_idk: str) -> TaskItem | None:
        data = get_latest_versions(
            session=Session,
            table='orcha.tasks',
            key_columns=['task_idk'],
            version_column='version',
            select_columns='*',
            match_pairs=[('task_idk', '=', task_idk)]
        )
        tasks = [TaskItem(**x) for x in data]

        if len(tasks) == 0:
            return None
        if len(tasks) > 1:
            raise Exception('Multiple tasks found with same idk')
        return tasks[0]

    @classmethod
    def create(
            cls, task_idk: str, name: str, description: str,
            schedule_sets: list[ScheduleSet], thread_group,
            task_function: Callable[[TaskItem | None, RunItem | None, dict], None],
            status: str = TaskStatus.ENABLED,
            task_metadata: dict = {},
            register_with_runner: bool = True
        ):
        """
        task_id: The unique id used to identify this task. This is used
            to identify the task in the database and in the task runner and
            for updates, enabling/disabling and deleting
        thread_group: The thread group to use for the task. All tasks in the
            same thread group will be run in the same thread.
        status: The status of the task. This can be used to disable a task when
            no longer required. Tasks must be explicitly disabled to prevent
            the scheduler from queuing runs for them.
        """

        if not is_initialised:
            raise Exception('orcha not initialised. Call orcha.initialise() first')

        version = dt.utcnow()
        current_task = TaskItem.get(task_idk)
        for schedule in schedule_sets:
            # set the set_idk as task_id+cron_schedule
            schedule.set_idk = f'{task_idk}_{schedule.cron_schedule}'

        update_needed = False
        if current_task is None:
            update_needed = True
        elif (
            current_task.task_metadata != task_metadata or
            current_task.name != name or
            current_task.description != description or
            current_task.schedule_sets != schedule_sets or
            current_task.thread_group != thread_group or
            current_task.status != status
        ):
            update_needed = True

        # Create and register the task with the task runner
        # before we check if it needs updating otherwise
        # we'll not register the task
        task = TaskItem(
            task_idk=task_idk,
            version=version,
            task_metadata=task_metadata,
            name=name,
            description=description,
            schedule_sets=schedule_sets,
            thread_group=thread_group,
            last_active=version,
            status=status,
            notes=None
        )

        task.task_function = task_function # type: ignore

        if register_with_runner:
            if _register_task_with_runner is None:
                raise Exception('No task runner registered')
            _register_task_with_runner(task)

        if not update_needed and current_task is not None:
            task.version = current_task.version
            return task
        else:
            task._update_db()
            return task


    def _update_db(self) -> None:
        with Session.begin() as session:
            session.merge(TaskRecord(
                task_idk = self.task_idk,
                version = self.version,
                task_metadata = self.task_metadata,
                name = self.name,
                description = self.description,
                schedule_sets = ScheduleSet.list_to_dict(self.schedule_sets),
                thread_group = self.thread_group,
                last_active = self.last_active,
                status = self.status,
                notes = self.notes
            ))

    def set_status(self, status: str, notes: str) -> None:
        """
        Used to enable/disable a task. This is used to prevent the scheduler
        from queuing runs for the task.
        """
        self.status = status
        self.notes = notes
        self._update_db()

    def update_active(self) -> None:
        """
        Used to indicate to the scheduler the last time the task was active.
        Old tasks that have not been active for a while will be automatically
        disabled by the scheduler.
        """
        self.last_active = dt.utcnow()
        self._update_db()

    def get_schedule_from_id(self, set_idk: str) -> ScheduleSet | None:
        for schedule in self.schedule_sets:
            if schedule.set_idk == set_idk:
                return schedule
        return None

    def get_last_scheduled(self, schedule: ScheduleSet) -> dt:
        cron_schedule = schedule.cron_schedule
        return croniter(cron_schedule, dt.now()).get_prev(dt)

    def get_time_between_runs(self, schedule: ScheduleSet) -> td:
        cron = croniter(schedule.cron_schedule)
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

    def get_last_run(self, schedule: ScheduleSet) -> RunItem | None:
        return RunItem.get_latest(task=self, schedule=schedule)

    def is_run_due(self, schedule: ScheduleSet):
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

    def schedule_run(self, schedule) -> RunItem:
        return RunItem.create(
            task=self,
            run_type='scheduled',
            scheduled_time=self.get_last_scheduled(schedule),
            schedule=schedule
        )

    def get_queued_runs(self) -> list[RunItem]:
        return RunItem.get_all_queued(
            task_id=self.task_idk,
            schedule=None
        )

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


class RunStatus():
    QUEUED = 'queued'
    RUNNING = 'running'
    SUCCESS = 'success'
    WARN = 'warn'
    FAILED = 'failed'
    CANCELLED = 'cancelled'

    def __init__(self, status: str, text: str) -> None:
        self.status = status
        self.text = text

RunType = Literal['scheduled', 'manual', 'retry']

@dataclass
class RunItem():
    _task: TaskItem
    run_idk: str
    task_idf: str
    set_idf: str
    run_type: str
    scheduled_time: dt
    start_time: dt | None
    end_time: dt | None
    last_active: dt | None
    config: dict
    status: str
    output: dict | None = None

    @staticmethod
    def _task_id_populate(task_id: str | None, task: TaskItem | None) -> tuple[str, TaskItem]:
        """
        Internal function. Populates the unprovided task_id or task used
        by various functions.
        """
        if task is not None:
            task_id = task.task_idk
        elif task_id is not None:
            task = TaskItem.get(task_id)
            if task is None:
                raise Exception('Task not found')
        else:
            raise Exception('Either task_id or task must be provided')

        return task_id, task

    @staticmethod
    def create(
            task: TaskItem, run_type: RunType,
            schedule: ScheduleSet, scheduled_time: dt
        ) -> RunItem:
        if not is_initialised:
            raise Exception('orcha not initialised. Call orcha.initialise() first')
        run_idk = str(uuid4())
        status = RunStatus.QUEUED

        if schedule.set_idk is None:
            raise Exception('Schedule set idk not set')

        item = RunItem(
            _task = task,
            run_idk = run_idk,
            task_idf = task.task_idk,
            set_idf = schedule.set_idk,
            run_type = run_type,
            scheduled_time = scheduled_time,
            start_time = None,
            end_time = None,
            last_active = None,
            config = schedule.config,
            status = status,
            output = None
        )

        item._update_db()
        return item

    @staticmethod
    def get_all(
            schedule: ScheduleSet | None, since: dt,
            task_id: str | None = None, task: TaskItem | None = None,
            run_type: RunType | None = None
        ) -> list[RunItem]:
        """
        Gets all runs for a task since a particular time (inclusive)
        for a particular schedule set (optional, None for all runs)
        """
        task_id, task = RunItem._task_id_populate(task_id, task)
        pairs = [
            ('task_idf', '=', task_id),
            ('scheduled_time', '>=', since.isoformat())
        ]
        if run_type is not None:
            pairs.append(('run_type', '=', run_type))
        if schedule is not None:
            pairs.append(('set_idf', '=', schedule.set_idk))

        data = get(
            session = Session,
            table='orcha.runs',
            select_columns='*',
            match_pairs=pairs,
        )
        return [RunItem(task, **x) for x in data]

    @staticmethod
    def get_all_queued(schedule: ScheduleSet | None, task_id: str | None = None, task: TaskItem | None = None) -> list[RunItem]:
        task_id, task = RunItem._task_id_populate(task_id, task)
        pairs = [
            ('task_idf', '=', task_id),
            ('status', '=', RunStatus.QUEUED)
        ]
        if schedule is not None:
            pairs.append(('set_idf', '=', schedule.set_idk))
        data = get(
            session = Session,
            table='orcha.runs',
            select_columns='*',
            match_pairs=pairs,
        )
        return [RunItem(task, **x) for x in data]

    @staticmethod
    def get_latest(
            schedule: ScheduleSet, task_id: str | None = None,
            task: TaskItem | None = None, run_type: RunType | None = None
        ) -> RunItem | None:
        task_id, task = RunItem._task_id_populate(task_id, task)
        # To keep query time less dependent on the number of runs in the database
        # we can use the last run time and the time between runs to get the
        # window where the last run should have occurred
        last_run_time = task.get_last_scheduled(schedule)
        time_between_runs = task.get_time_between_runs(schedule)
        runs = RunItem.get_all(
            task=task,
            since=last_run_time - time_between_runs*2,
            schedule=schedule,
            run_type=run_type
        )
        if len(runs) == 0:
            # If we didn't get any runs - e.g. when the runner is started up
            # then query the full time window for any last run
            runs = RunItem.get_all(task=task, since=dt.min, schedule=schedule)
        # drop runs that aren't of the right type
        if run_type is not None:
            runs = [x for x in runs if x.run_type == run_type]

        if len(runs) == 0:
            return None
        # order runs by scheduled_time
        runs = sorted(runs, key=lambda x: x.scheduled_time, reverse=True)
        return runs[0]

    @staticmethod
    def get_by_id(run_id: str, task: TaskItem | None = None) -> RunItem | None:
        data = get(
            session = Session,
            table='orcha.runs',
            select_columns='*',
            match_pairs=[
                ('run_idk', '=', run_id)
            ],
        )
        if len(data) == 0:
            return None
        row_dict = dict(data[0])
        task_idf = row_dict.get('task_idf', None)
        if task is None:
            if task_idf is None:
                raise Exception('task_idf not found in run data')
            task = TaskItem.get(task_idf)
            if task is None:
                raise Exception('Task not found')
        return RunItem(task, **data[0])

    def _update_db(self):
        with Session.begin() as session:
            session.merge(RunRecord(
                run_idk = self.run_idk,
                task_idf = self.task_idf,
                set_idf = self.set_idf,
                run_type = self.run_type,
                scheduled_time = self.scheduled_time,
                start_time = self.start_time,
                end_time = self.end_time,
                last_active = self.last_active,
                config = self.config,
                status = self.status,
                output = self.output
            ))

    def update_active(self):
        self.last_active = dt.utcnow()
        self._update_db()

    def update(
            self, status: str, start_time: dt | None ,
            end_time: dt | None, output: dict | None = None
        ):
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.output = output

        db_data = RunItem.get_by_id(self.run_idk, task=self._task)

        needs_update = False
        if db_data is None:
            needs_update = True
        elif(
            db_data.status != self.status or
            db_data.start_time != self.start_time or
            db_data.end_time != self.end_time or
            db_data.output != self.output
        ):
            needs_update = True

        if needs_update:
            self._update_db()

    def set_running(self, output: dict | None = None):
        db_item = RunItem.get_by_id(self.run_idk, task=self._task)
        if db_item is not None:
            if db_item.status == RunStatus.RUNNING:
                # if it's already set, we don't
                # want to update it again
                return
        self.update(
            status = RunStatus.RUNNING,
            start_time = dt.utcnow(),
            end_time = None,
            output = output
        )

    def set_success(self, output: dict | None = None):
        db_item = RunItem.get_by_id(self.run_idk, task=self._task)
        if db_item is not None:
            if db_item.status == RunStatus.SUCCESS:
                # if it's already set, we don't
                # want to update it again
                return
        self.update(
            status = RunStatus.SUCCESS,
            start_time = self.start_time,
            end_time = dt.utcnow(),
            output = output
        )

    def set_failed(self, output: dict | None = None):
        db_item = RunItem.get_by_id(self.run_idk, task=self._task)
        if db_item is not None:
            if db_item.status == RunStatus.FAILED:
                # if it's already set, we don't
                # want to update it again
                return
        self.update(
            status = RunStatus.FAILED,
            start_time = self.start_time,
            end_time = dt.utcnow(),
            output = output
        )

    def set_output(self, output: dict | None = None, merge = False):
        """
        Sets the output for the run. This will overwrite any existing output.
        If merge is set to True then the output will be merged with any
        existing output.
        """
        db_item = RunItem.get_by_id(self.run_idk, task=self._task)
        new_output = output
        if merge and db_item is not None:
            new_output = db_item.output
            if new_output is None:
                new_output = {}
            if output is not None:
                new_output.update(output)
        if db_item is None:
            raise Exception('update_output failed, run not found')
        self.update(
            status = db_item.status,
            start_time = db_item.start_time,
            end_time = db_item.end_time,
            output = output
        )
