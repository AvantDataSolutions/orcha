from __future__ import annotations

import threading
import time

from orcha.core.tasks import RunStatus, TaskItem
from orcha.utils.log import LogManager

orcha_log = LogManager('orcha')

# TODO terminate nicely: https://itnext.io/containers-terminating-with-grace-d19e0ce34290
# TODO https://docs.docker.com/engine/reference/commandline/stop/


class Scheduler:

    all_tasks: list[TaskItem] = []
    last_refresh: float = 0
    task_refresh_interval: float
    fail_unstarted_runs: bool = True
    disable_stale_tasks: bool = True

    def __init__(
            self,
            fail_unstarted_runs: bool = True,
            disable_stale_tasks: bool = True,
        ):
        """
        Initialise the scheduler with the given settings. The scheduler creates
        threads and creates runs in the database for tasks that are due to run.
        :param fail_unstarted_runs: If True, then if a run is due, but the last
        run didn't start, then the last run will be set to failed before a new
        run is created.
        :param disable_stale_tasks: If True, then if a task hasn't been active
        since the last run, then the task will be set to inactive.
        """
        self.is_running = False
        self.thread = None
        self.fail_unstarted_runs = fail_unstarted_runs
        self.disable_stale_tasks = disable_stale_tasks

    def start(self, refresh_interval: float = 60):
        orcha_log.add_entry('scheduler', 'status', 'Starting', {})
        self.is_running = True
        self.task_refresh_interval = refresh_interval
        self.thread = threading.Thread(target=self._run)
        self.thread.start()
        return self.thread

    def stop(self):
        orcha_log.add_entry('scheduler', 'status', 'Stopping', {})
        self.is_running = False
        if self.thread is not None:
            self.thread.join()

    def _run(self):
        while self.is_running:
            time.sleep(15)
            if self.last_refresh < time.time() - self.task_refresh_interval:
                orcha_log.add_entry('scheduler', 'run', 'Refreshing tasks', {})
                self.last_refresh = time.time()
                self.all_tasks = TaskItem.get_all()
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