
import threading
import time

from orcha.core.tasks import TaskItem, TaskStatus, RunStatus

# TODO terminate nicely: https://itnext.io/containers-terminating-with-grace-d19e0ce34290
# TODO https://docs.docker.com/engine/reference/commandline/stop/


class Scheduler:

    all_tasks: list[TaskItem] = []
    last_refresh: float = 0
    task_refresh_interval: float
    fail_unstarted_runs: bool = True
    disable_stale_tasks: bool = True

    def __init__(self, fail_unstarted_runs: bool = True, disable_stale_tasks: bool = True):
        self.is_running = False
        self.thread = None
        self.fail_unstarted_runs = fail_unstarted_runs
        self.disable_stale_tasks = disable_stale_tasks

    def start(self, refresh_interval: float = 60):
        self.is_running = True
        self.task_refresh_interval = refresh_interval
        self.thread = threading.Thread(target=self._run)
        self.thread.start()
        return self.thread

    def stop(self):
        self.is_running = False
        if self.thread is not None:
            self.thread.join()

    def _run(self):
        while self.is_running:
            time.sleep(10)
            if self.last_refresh < time.time() - self.task_refresh_interval:
                self.last_refresh = time.time()
                self.all_tasks = TaskItem.get_all()
            elif len(self.all_tasks) == 0:
                self.last_refresh = time.time()
                self.all_tasks = TaskItem.get_all()

            for task in self.all_tasks:
                if task.status == TaskStatus.DISABLED:
                    continue
                is_due, last_run =  task.is_run_due_with_last()
                if is_due:
                    if self.fail_unstarted_runs and last_run is not None:
                        # If the last run didn't start, and it's not already
                        # failed, set it to failed before we create a new run
                        if last_run.start_time is None and last_run.status != RunStatus.FAILED:
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
                            task.set_status('disabled', 'Task disabled due to inactivity')
                            continue
                    print('Run due for task:', task.task_idk)
                    run = task.schedule_run()