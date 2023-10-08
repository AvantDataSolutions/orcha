from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime as dt, timedelta as td
from typing import Callable
from uuid import uuid4

from croniter import croniter
from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.postgresql import JSON as PG_JSON
from sqlalchemy.engine.row import Row

from orcha.utils.sqlalchemy import (get, get_latest_versions, postgres_build,
                                    postgres_scaffold)

from .credentials import *

print('Loading dh:',__name__)

CUR_SCHEMA = 'orcha'
Base, engine, Session = postgres_scaffold(CUR_SCHEMA)


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


class TaskRecord(Base):
    __tablename__ = 'tasks'

    task_idk = Column(String, primary_key=True)
    version = Column(DateTime(timezone=False), primary_key=True)
    name = Column(String)
    description = Column(String)
    cron_schedule = Column(String)
    thread_group = Column(String)
    last_active = Column(DateTime(timezone=False))
    status = Column(String)
    notes = Column(String)


@dataclass
class TaskItem():
    task_idk: str
    version: dt
    name: str
    description: str
    cron_schedule: str
    thread_group: str
    last_active: dt
    status: str
    notes: str | None = None

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
            cron_schedule: str, thread_group,
            task_function: Callable[[TaskItem | None, RunItem | None], None],
            status: str = TaskStatus.ENABLED
        ):
        cls.task_function = task_function    # type: ignore
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
        version = dt.utcnow()

        current_task = TaskItem.get(task_idk)

        update_needed = False
        if current_task is None:
            update_needed = True
        elif (
            current_task.name != name or
            current_task.description != description or
            current_task.cron_schedule != cron_schedule or
            current_task.thread_group != thread_group or
            current_task.status != status
        ):
            update_needed = True

        if not update_needed and current_task is not None:
            return current_task

        task = TaskItem(
            task_idk=task_idk,
            version=version,
            name=name,
            description=description,
            cron_schedule=cron_schedule,
            thread_group=thread_group,
            last_active=version,
            status=status,
            notes=None
        )

        task._update_db()
        return task


    def _update_db(self) -> None:
        with Session.begin() as session:
            session.merge(TaskRecord(
                task_idk = self.task_idk,
                version = self.version,
                name = self.name,
                description = self.description,
                cron_schedule = self.cron_schedule,
                thread_group = self.thread_group,
                last_active = self.last_active,
                status = self.status,
                notes = self.notes
            ))
            session.commit()

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


    def get_last_scheduled(self) -> dt:
        return croniter(self.cron_schedule, dt.now()).get_prev(dt)

    def get_time_between_runs(self) -> td:
        cron = croniter(self.cron_schedule)
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

    def get_last_run(self) -> RunItem | None:
        return RunItem.get_latest(task=self)

    def is_run_due(self):
        is_due, _ = self.is_run_due_with_last()
        return is_due

    def is_run_due_with_last(self) -> tuple[bool, RunItem | None]:
        """
        Returns if a run is due and the last run instance
        to save calling get_last_run twice
        Returns a tuple of (is_due, last_run)
        """
        last_run = RunItem.get_latest(task=self)
        if last_run is None:
            return True, last_run
        return last_run.scheduled_time < self.get_last_scheduled(), last_run

    def schedule_run(self) -> RunItem:
        return RunItem.create(
            task=self,
            scheduled_time=self.get_last_scheduled()
        )

    def get_queued_runs(self) -> list[RunItem]:
        return RunItem.get_all_queued(
            task_id=self.task_idk
        )

    def task_function(self, run: RunItem | None) -> None:
        """
        The Orcha task runner will pass the current run instance
        to this function which can be used to update the run status
        as required, however the function can be manually called with
        no run instance if required (e.g. for testing)
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


class RunRecord(Base):
    __tablename__ = 'runs'

    run_idk = Column(String, primary_key=True)
    task_idf = Column(String)
    scheduled_time = Column(DateTime(timezone=False))
    start_time = Column(DateTime(timezone=False))
    end_time = Column(DateTime(timezone=False))
    last_active = Column(DateTime(timezone=False))
    status = Column(String)
    output = Column(PG_JSON)


@dataclass
class RunItem():
    _task: TaskItem
    run_idk: str
    task_idf: str
    scheduled_time: dt
    start_time: dt | None
    end_time: dt | None
    last_active: dt | None
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
    def create(task: TaskItem, scheduled_time: dt) -> RunItem:
        run_idk = str(uuid4())
        status = RunStatus.QUEUED

        item = RunItem(
            _task = task,
            run_idk = run_idk,
            task_idf = task.task_idk,
            scheduled_time = scheduled_time,
            start_time = None,
            end_time = None,
            last_active = None,
            status = status,
            output = None
        )

        item._update_db()
        return item

    @staticmethod
    def get_all(since: dt, task_id: str | None = None, task: TaskItem | None = None) -> list[RunItem]:
        task_id, task = RunItem._task_id_populate(task_id, task)
        data = get(
            session = Session,
            table='orcha.runs',
            select_columns='*',
            match_pairs=[
                ('task_idf', '=', task_id),
                ('scheduled_time', '>=', since.isoformat())
            ],
        )
        return [RunItem(task, **x) for x in data]

    @staticmethod
    def get_all_queued(task_id: str) -> list[RunItem]:
        task_id, task = RunItem._task_id_populate(task_id, None)
        data = get(
            session = Session,
            table='orcha.runs',
            select_columns='*',
            match_pairs=[
                ('task_idf', '=', task_id),
                ('status', '=', RunStatus.QUEUED)
            ],
        )
        return [RunItem(task, **x) for x in data]

    @staticmethod
    def get_latest(task_id: str | None = None, task: TaskItem | None = None) -> RunItem | None:
        task_id, task = RunItem._task_id_populate(task_id, task)
        # To keep query time less dependent on the number of runs in the database
        # we can use the last run time and the time between runs to get the
        # window where the last run should have occurred
        last_run_time = task.get_last_scheduled()
        time_between_runs = task.get_time_between_runs()
        runs = RunItem.get_all(task=task, since=last_run_time - time_between_runs*2)
        if len(runs) == 0:
            # If we didn't get any runs - e.g. when the runner is started up
            # then query the full time window for any last run
            runs = RunItem.get_all(task=task, since=dt.min)
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
                scheduled_time = self.scheduled_time,
                start_time = self.start_time,
                end_time = self.end_time,
                last_active = self.last_active,
                status = self.status,
                output = self.output
            ))
            session.commit()

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


postgres_build(Base, engine, CUR_SCHEMA)