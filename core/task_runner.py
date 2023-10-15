
import time
import threading

from orcha.core import tasks
from orcha.core.tasks import RunStatus, TaskItem, RunItem

# TODO terminate nicely
# https://itnext.io/containers-terminating-with-grace-d19e0ce34290
# https://docs.docker.com/engine/reference/commandline/stop/

BASE_THREAD_GROUP = 'base_thread'


class ThreadHandler():

    def __init__(self, thread_group: str):
        self.is_running = False
        self.thread_group = thread_group
        self.thread = None
        self.tasks: list[TaskItem] = []

    def start(self):
        self.is_running = True
        if self.thread is not None:
            raise Exception('Thread already started')
        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def stop(self):
        self.is_running = False
        if self.thread is not None:
            self.thread.join()

    def add_task(self, task: TaskItem):
        self.tasks.append(task)

    def _run(self):
        while self.is_running:
            for task in self.tasks:
                TaskRunner.process_task(task)
            time.sleep(15)


class TaskRunner():

    handlers: dict[str, ThreadHandler] = {}

    run_in_thread: bool = True
    use_thread_groups: bool = True

    @staticmethod
    def process_task(task: TaskItem):
        # Set the task as active so the scheduler doesn't disable it
        task.update_active()
        # Run in a second thread to tick over the active time
        # this is mostly here if something crashes and the
        # task never finishes, so we can check for stale active
        # times and deal with it accordingly
        running_dict = {}
        def _refresh_active(run: RunItem):
            while running_dict[run.run_idk]:
                run.update_active()
                time.sleep(30)

        # We only want to run the last run if its queued. If we're backing up
        # runs then the user needs to manage how long runs are taking and how
        # the schedule it's being run on
        last_run = task.get_last_run()
        if last_run is None or last_run.status != RunStatus.QUEUED:
            return
        # queued_runs = task.get_queued_runs()
        queued_runs = [last_run]
        for run in queued_runs:
            # Set the run as started so when we update the active time it
            # has the version that has already started otherwise it will
            # set the active time on the unstarted version of the run
            run.set_running()
            running_dict[run.run_idk] = True
            ra_thread = threading.Thread(target=_refresh_active, args=(run,))
            ra_thread.start()
            # print(f'Running task {task.name} with run_id {run.run_idk}')
            try:
                # print('Running task:', task.task_idk)
                task.task_function(task, run)
                running_dict[run.run_idk] = False
            except Exception as e:
                # print(f'Error running task {task.name} with run_id {run.run_idk}, with exception: {str(e)}')
                run.set_failed(output={
                    'exception': str(e),
                })
                running_dict[run.run_idk] = False
                continue
            # print(f'Finished task {task.name} with run_id {run.run_idk}')

    def __init__(self, run_in_thread = True, use_thread_groups = True, default_runner = True):
        self.run_in_threads = run_in_thread
        self.use_thread_groups = use_thread_groups
        if default_runner:
            if tasks._register_task_with_runner is not None:
                raise Exception('Default task runner already set')
            tasks._register_task_with_runner = self.register_task

    def register_task(self, task: TaskItem):
        # TODO: check if task is already registered
        # If we're not using thread groups
        # then we default to using the base thread
        if self.use_thread_groups:
            thread_group = task.thread_group
        else:
            thread_group = BASE_THREAD_GROUP

        if thread_group not in self.handlers:
            self.handlers[thread_group] = ThreadHandler(task.thread_group)
            if self.run_in_threads:
                self.handlers[thread_group].start()

        self.handlers[thread_group].add_task(task)

    def register_tasks(self, tasks: list[TaskItem]):
        for task in tasks:
            self.register_task(task)

    def process_all_tasks(self):
        for handler in self.handlers.values():
            for task in handler.tasks:
                TaskRunner.process_task(task)

    def stop_all(self, stop_base = False):
        for handler in self.handlers.values():
            if handler.thread_group == BASE_THREAD_GROUP and not stop_base:
                continue
            handler.stop()
