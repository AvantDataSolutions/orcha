from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import timedelta as td

from orcha.core.tasks import RunStatus, TaskItem
from orcha.utils.log import LogManager

orcha_log = LogManager('orcha')

# TODO terminate nicely: https://itnext.io/containers-terminating-with-grace-d19e0ce34290
# TODO https://docs.docker.com/engine/reference/commandline/stop/


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
        - fail_historical_interval(float = 43200): The interval in seconds at which the scheduler will check.
        """
        task_refresh_interval: float = 60
        fail_unstarted_runs: bool = True
        disable_stale_tasks: bool = True
        prune_runs_max_age: td | None = td(days=180)
        prune_logs_max_age: td | None = td(days=180)
        prune_interval: float = 3600
        fail_historical_runs: bool = True
        fail_historical_age: td | None = td(hours=24)
        fail_historical_interval: float = 3600


class Scheduler:

    all_tasks: list[TaskItem] = []
    last_refresh: float = 0
    task_refresh_interval: float
    fail_unstarted_runs: bool
    disable_stale_tasks: bool
    prune_runs_max_age: td | None
    prune_logs_max_age: td | None
    prune_interval: float
    fail_historical_runs: bool
    fail_historical_age: td | None
    fail_historical_interval: float

    def __init__(
            self,
            config: OrchaSchedulerConfig = OrchaSchedulerConfig(),
            fail_unstarted_runs: bool | None = None,
            disable_stale_tasks: bool | None = None,
        ):
        """
        Initialise the scheduler with the given settings. The scheduler creates
        threads and creates runs in the database for tasks that are due to run.
        ### Args
        - config(OrchaSchedulerConfig | None = None): The configuration for the scheduler.
        - fail_unstarted_runs: If True, then if a run is due, but the last
        run didn't start, then the last run will be set to failed before a new
        run is created.
        - disable_stale_tasks: If True, then if a task hasn't been active
        since the last run, then the task will be set to inactive.
        """
        self.is_running = False
        self.thread = None

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

    def start(self, refresh_interval: float = 60):
        orcha_log.add_entry('scheduler', 'status', 'Starting', {})
        self.is_running = True
        # Start the run scheduling thread
        self.task_refresh_interval = refresh_interval
        self.thread = threading.Thread(target=self._run)
        self.thread.start()
        # Start the run pruning thread
        prune_thread = threading.Thread(target=self._prune_runs_and_logs)
        prune_thread.start()
        # Start the historical run failure thread
        fail_historical_thread = threading.Thread(target=self._fail_historical)
        fail_historical_thread.start()
        return self.thread

    def stop(self):
        orcha_log.add_entry('scheduler', 'status', 'Stopping', {})
        self.is_running = False
        if self.thread is not None:
            self.thread.join()

    def _prune_runs_and_logs(self):
        while self.is_running:
            time.sleep(self.prune_interval)
            if self.prune_runs_max_age is not None:
                for task in self.all_tasks:
                    del_count = task.prune_runs(self.prune_runs_max_age)
                    orcha_log.add_entry('scheduler', 'prune_runs', 'Pruning runs', {
                        'task_id': task.task_idk,
                        'max_age': str(self.prune_runs_max_age),
                        'deleted_count': del_count
                    })
            if self.prune_logs_max_age is not None:
                del_count = orcha_log.prune(self.prune_logs_max_age)
                orcha_log.add_entry('scheduler', 'prune_logs', 'Pruning logs', {
                    'max_age': str(self.prune_logs_max_age),
                    'deleted_count': del_count
                })

    def _fail_historical(self):
        while self.is_running:
            time.sleep(self.fail_historical_interval)
            if self.fail_historical_runs and self.fail_historical_age is not None:
                for task in self.all_tasks:
                    open_runs = task.get_running_runs() + task.get_queued_runs()
                    historical_count = 0
                    for run in open_runs:
                        run_age = dt.utcnow() - run.scheduled_time
                        if run_age > self.fail_historical_age:
                            run.set_failed(
                                output={
                                    'message': 'Historical run failed to start/finish'
                                },
                                zero_duration=True
                            )
                            historical_count += 1
                    orcha_log.add_entry('scheduler', 'fail_historical_runs', 'Failing historical runs', {
                        'task_id': task.task_idk,
                        'max_age': str(self.fail_historical_age),
                        'failed_count': historical_count
                    })

    def _process_schedules(self):
        while self.is_running:
            time.sleep(15)
            if self.last_refresh < time.time() - self.task_refresh_interval:
                self.all_tasks = TaskItem.get_all()
                orcha_log.add_entry('scheduler', 'run', 'Refreshing tasks', {
                    'task_count': len(self.all_tasks)
                })
                self.last_refresh = time.time()
            elif len(self.all_tasks) == 0:
                self.last_refresh = time.time()
                self.all_tasks = TaskItem.get_all()

            for task in self.all_tasks:
                for schedule in task.schedule_sets:
                    # Only check enabled tasks (e.g. no disabled/inactive tasks)
                    if task.status != 'enabled':
                        continue
                    is_due, last_run =  task.is_run_due_with_last(schedule)
                    if is_due:
                        # TODO Check for old queued/running runs and set them to failed
                        if self.fail_unstarted_runs and last_run is not None:
                            # If the last run is still queued then set it to failed
                            # before we create a new run
                            if last_run.start_time is None and (last_run.status == RunStatus.QUEUED):
                                last_run.set_failed(
                                    output={
                                        'message': 'Previous run failed to start'
                                    }
                                )
                        if self.disable_stale_tasks and last_run is not None:
                            # If the task hasn't been active since the last run,
                            # then it's stale and should be disabled.
                            # Tasks should be checked every 5s, and runs at most frequent, every 1 minute
                            # so a task should have been active many times since the last run
                            if task.last_active < last_run.scheduled_time:
                                task.set_status('inactive', 'Task has been inactive since last scheduled run')
                                continue
                        # print('Run due for task:', task.task_idk)
                        run = task.schedule_run(schedule)