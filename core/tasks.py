from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime as dt
from datetime import timedelta as td
from typing import Callable, Literal
from uuid import uuid4

from croniter import croniter
from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.postgresql import JSON as PG_JSON
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import text as sql

from orcha.utils.sqlalchemy import (
    get,
    get_latest_versions,
    postgres_scaffold,
    sqlalchemy_build,
)

print('Loading:',__name__)

is_initialised = False

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
        '''))

"""
===================================================================
 Task Item classes and definitions
===================================================================
"""


TaskStatus = Literal['enabled', 'disabled', 'inactive', 'deleted']


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
    task_tags: list[str]
    name: str
    description: str
    schedule_sets: list[ScheduleSet]
    thread_group: str
    last_active: dt
    status: TaskStatus
    notes: str | None = None

    def __init__(
            self, task_idk: str, version: dt, task_metadata: dict, name: str,
            description: str, schedule_sets: list[ScheduleSet] | list[dict],
            thread_group: str, last_active: dt, status: TaskStatus,
            task_tags: list[str], notes: str | None = None
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
                sets.append(ScheduleSet.create_with_key(**schedule_set))
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

    @staticmethod
    def get_all() -> list[TaskItem]:
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

        confirm_initialised()

        version = dt.now()
        current_task = TaskItem.get(task_idk)
        new_s_sets: list[ScheduleSet] = []
        for schedule in schedule_sets:
            # set the set_idk as task_id+cron_schedule
            new_s_sets.append(ScheduleSet.create_with_key(
                set_idk=f'{task_idk}_{schedule.cron_schedule}',
                cron_schedule=schedule.cron_schedule,
                config=schedule.config
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
        with s_maker.begin() as session:
            session.merge(TaskRecord(
                task_idk = self.task_idk,
                version = self.version,
                task_metadata = self.task_metadata,
                task_tags = self.task_tags,
                name = self.name,
                description = self.description,
                schedule_sets = ScheduleSet.list_to_dict(self.schedule_sets),
                thread_group = self.thread_group,
                last_active = self.last_active,
                status = self.status,
                notes = self.notes
            ))

    def set_status(self, status: TaskStatus, notes: str) -> None:
        """
        Used to enable/disable a task. This is used to prevent the scheduler
        from queuing runs for the task.
        """
        self.status = status
        self.notes = notes
        # Toggling status will create a new version
        self.version = dt.now()
        self._update_db()

    def set_enabled(self, notes: str) -> None:
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
        self.last_active = dt.now()
        self._update_db()

    def get_schedule_set(self, set_idk: str) -> ScheduleSet | None:
        for schedule in self.schedule_sets:
            if schedule.set_idk == set_idk:
                return schedule
        return None

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

    def get_last_run(self, schedule: ScheduleSet | None) -> RunItem | None:
        return RunItem.get_latest(task=self, schedule=schedule)

    def get_next_scheduled_time(self, schedule: ScheduleSet | None = None) -> dt:
        """
        Returns the next scheduled time for the task. If no schedule is
        provided then the first schedule set is used.
        """
        if schedule is None:
            schedule = self.schedule_sets[0]
        cron_schedule = schedule.cron_schedule
        return croniter(cron_schedule, dt.now()).get_next(dt)

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

    def schedule_run(self, schedule: ScheduleSet) -> RunItem:
        """
        Schedules a run for the task and schedule set and returns the run instance.
        This creates a run regardless of whether a run is due or not.
        """
        return RunItem.create(
            task=self,
            run_type='scheduled',
            scheduled_time=self.get_last_scheduled(schedule),
            schedule=schedule
        )

    def get_queued_runs(self) -> list[RunItem]:
        return RunItem.get_all_queued(task=self)

    def get_running_runs(self) -> list[RunItem]:
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
                'date_cutoff': dt.now() - max_age
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


class RunStatus():
    """
    The available statuses for a run instance.
    """
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
"""
The types of runs that can be created.
- scheduled: A run that is created by the scheduler
- manual: A run that is created manually as a 'one-off'
- retry: A run that is created as a retry of a failed run
"""

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
        - scheduled_time: The time the run was scheduled to run
        - start_time: The time the run started
        - end_time: The time the run ended
        - last_active: The last time the run was active
        - config: The config for the run from the schedule set
        - status: The status of the run (queued, running, success, warn, failed, cancelled)
        - output: The output of the run which includes all outputs from modules and the task function
    """

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
    def create(
            task: TaskItem, run_type: RunType,
            schedule: ScheduleSet, scheduled_time: dt
        ) -> RunItem:
        """
        Creates a new run instance for a task with a new uuid and
        'new run' defaults in the database. This is a separate function to the
        __init__ to keep creating database entries separate from instanciating.
        """
        confirm_initialised()

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
            task: str | TaskItem,
            since: dt,
            schedule: ScheduleSet | None = None,
            run_type: RunType | None = None
        ) -> list[RunItem]:
        """
        Gets all runs for a task since a particular time (inclusive)
        for a particular schedule set (optional, None for all runs)
        """
        confirm_initialised()
        task = RunItem._task_id_populate(task)

        # make sure the schedule set is for this task
        task_schedule_sets = [x.set_idk for x in task.schedule_sets]
        if schedule is not None and schedule.set_idk not in task_schedule_sets:
            raise Exception('Schedule set not found for task')
        pairs = [
            ('task_idf', '=', task.task_idk),
            ('scheduled_time', '>=', since.isoformat())
        ]
        if run_type is not None:
            pairs.append(('run_type', '=', run_type))
        if schedule is not None:
            pairs.append(('set_idf', '=', schedule.set_idk))
        data = get(
            s_maker = s_maker,
            table='orcha.runs',
            select_columns='*',
            match_pairs=pairs,
        )
        return [RunItem(task, **(x._asdict())) for x in data]

    @staticmethod
    def get_all_queued(
            task: str | TaskItem,
            schedule: ScheduleSet | None = None,
        ) -> list[RunItem]:
        confirm_initialised()
        task = RunItem._task_id_populate(task)
        pairs = [
            ('task_idf', '=', task.task_idk),
            ('status', '=', RunStatus.QUEUED)
        ]
        if schedule is not None:
            pairs.append(('set_idf', '=', schedule.set_idk))
        data = get(
            s_maker = s_maker,
            table='orcha.runs',
            select_columns='*',
            match_pairs=pairs,
        )
        return [RunItem(task, **(x._asdict())) for x in data]

    @staticmethod
    def get_running_runs(
            task: str | TaskItem,
            schedule: ScheduleSet | None = None,
        ) -> list[RunItem]:
        confirm_initialised()
        task = RunItem._task_id_populate(task)
        pairs = [
            ('task_idf', '=', task.task_idk),
            ('status', '=', RunStatus.RUNNING)
        ]
        if schedule is not None:
            pairs.append(('set_idf', '=', schedule.set_idk))
        data = get(
            s_maker = s_maker,
            table='orcha.runs',
            select_columns='*',
            match_pairs=pairs,
        )
        return [RunItem(task, **(x._asdict())) for x in data]

    @staticmethod
    def get_latest(
            task: str | TaskItem,
            schedule: ScheduleSet | None = None,
            run_type: RunType | None = None
        ) -> RunItem | None:
        confirm_initialised()
        task = RunItem._task_id_populate(task)
        # To keep query time less dependent on the number of runs in the database
        # we can use the last run time and the time between runs to get the
        # window where the last run should have occurred
        if schedule is not None:
            last_run_time = task.get_last_scheduled(schedule)
            time_between_runs = task.get_time_between_runs(schedule)
            runs = RunItem.get_all(
                task=task,
                since=last_run_time - time_between_runs*2,
                schedule=schedule,
                run_type=run_type
            )
        else:
            # if we don't have a schedule given, then let the below get_all
            # grab all runs and filter them
            runs = []
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
    def get(run_id: str, task: TaskItem | None = None) -> RunItem | None:
        confirm_initialised()
        data = get(
            s_maker = s_maker,
            table='orcha.runs',
            select_columns='*',
            match_pairs=[
                ('run_idk', '=', run_id)
            ],
        )
        if len(data) == 0:
            return None
        task_idf = None
        if hasattr(data[0], 'task_idf'):
            task_idf = data[0].task_idf
        if task is None:
            if task_idf is None:
                raise Exception('task_idf not found in run data')
            task = TaskItem.get(task_idf)
            if task is None:
                raise Exception('Task not found')
        return RunItem(task, **(data[0]._asdict()))

    def reload(self):
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

    def _update_db(self):
        with s_maker.begin() as session:
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
        self.last_active = dt.now()
        self._update_db()

    def update(
            self, status: str, start_time: dt | None ,
            end_time: dt | None, output: dict | None = None
        ):
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.output = output

        db_data = RunItem.get(self.run_idk, task=self._task)

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

    def set_running(self, output: dict = {}):
        """
        Sets the run as running and sets the start time.
        Merges the output with any existing output.
        """
        db_item = RunItem.get(self.run_idk, task=self._task)
        if db_item is not None:
            if db_item.status == RunStatus.RUNNING:
                # if it's already set, we don't
                # want to update it again
                return
            if db_item.status != RunStatus.QUEUED:
                raise Exception('Run status is not queued, cannot set to running')
            if db_item.output is not None:
                output.update(db_item.output)

        self.update(
            status = RunStatus.RUNNING,
            start_time = dt.now(),
            end_time = None,
            output = output
        )

    def set_success(self, output: dict = {}):
        """
        Sets the run as success and sets the end time.
        Merges the output with any existing output.
        This will not overwrite an existing FAILED or WARN state, and
        effectively only go from QUEUED or RUNNING to SUCCESS.
        """
        db_item = RunItem.get(self.run_idk, task=self._task)
        if db_item is not None:
            if db_item.status == RunStatus.FAILED:
                # If a run has failed (e.g. timeout) then leave it has failed
                raise Exception('Run status set to failed, cannot set to success')
            elif db_item.status == RunStatus.WARN:
                # If a run has a warning then leave it as a warning
                raise Exception('Run status set to warn, cannot set to success')
            elif db_item.status == RunStatus.SUCCESS:
                # if it's already set, we don't
                # want to update it again
                return

            if db_item.output is not None:
                output.update(db_item.output)

        self.update(
            status = RunStatus.SUCCESS,
            start_time = self.start_time,
            end_time = dt.now(),
            output = output
        )

    def set_warn(self, output: dict = {}):
        """
        Sets the run as a warning and sets the end time.
        Merges the output with any existing output.
        This will not overwrite an existing FAILED state.
        """
        db_item = RunItem.get(self.run_idk, task=self._task)
        if db_item is not None:
            if db_item.status == RunStatus.FAILED:
                # If a run has failed (e.g. timeout) then leave it has failed
                raise Exception('Run status set to failed, cannot set to warn')
            elif db_item.status == RunStatus.WARN:
                # if it's already set, we don't
                # want to update it again
                return
            if db_item.output is not None:
                output.update(db_item.output)

        self.update(
            status = RunStatus.WARN,
            start_time = self.start_time,
            end_time = dt.now(),
            output = output
        )

    def set_failed(self, output: dict = {}, zero_duration = False):
        """
        Sets the run as failed and sets the end time.
        Merges the output with any existing output.
        Optionally can fail the run with a zero duration, useful when
        failing historical runs as we don't know when they actually stopped.
        """
        db_item = RunItem.get(self.run_idk, task=self._task)
        if db_item is not None:
            if db_item.status == RunStatus.CANCELLED:
                # If a run has been cancelled then leave it as cancelled
                return
            if db_item.status == RunStatus.FAILED:
                # if it's already set, we don't
                # want to update it again
                return
            if db_item.output is not None:
                output.update(db_item.output)

        failed_time = dt.now()
        if zero_duration:
            failed_time = self.start_time

        self.update(
            status = RunStatus.FAILED,
            start_time = self.start_time,
            end_time = failed_time,
            output = output
        )

    def set_cancelled(self, output: dict = {}, zero_duration = False):
        """
        Sets the run as cancelled and sets the end time.
        Merges the output with any existing output.
        """
        db_item = RunItem.get(self.run_idk, task=self._task)
        if db_item is not None:
            if db_item.status == RunStatus.CANCELLED:
                # if it's already set, we don't
                # want to update it again
                return
            if db_item.output is not None:
                output.update(db_item.output)


        cancelled_time = dt.now()
        if zero_duration:
            cancelled_time = self.start_time

        self.update(
            status = RunStatus.CANCELLED,
            start_time = self.start_time,
            end_time = cancelled_time,
            output = output
        )

    def set_output(self, output: dict | None, merge = False):
        """
        Sets the output for the run. This will overwrite any existing output and
        and existing state (e.g. FAILED -> SUCCESS).
        If merge is set to True then the output will be merged with any
        existing output.
        """
        db_item = RunItem.get(self.run_idk, task=self._task)
        if db_item is None:
            raise Exception('update_output failed, run not found')
        new_output = output
        if merge:
            new_output = db_item.output
            if new_output is None:
                new_output = {}
            if output is not None:
                new_output.update(output)

        self.update(
            status = db_item.status,
            start_time = db_item.start_time,
            end_time = db_item.end_time,
            output = new_output
        )
