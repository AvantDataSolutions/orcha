
import time
import threading

from orcha.core import tasks
from orcha.core.tasks import TaskItem, RunItem
from orcha.utils import kvdb
from orcha.utils.threading import run_function_with_timeout, run_function_store_exception

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
        self.thread = threading.Thread(target=self._run, name=self.thread_group)
        self.thread.start()

    def stop(self):
        self.is_running = False
        if self.thread is not None:
            self.thread.join()

    def add_task(self, task: TaskItem):
        # check if the task is in the list by id and if not append it
        task_ids = [t.task_idk for t in self.tasks]
        if task.task_idk not in task_ids:
            self.tasks.append(task)
        else:
            # replace the task if it's already in the list
            for i, t in enumerate(self.tasks):
                if t.task_idk == task.task_idk:
                    self.tasks[i] = task

    def _run(self):
        while self.is_running:
            for task in self.tasks:
                # Set the task as active so the scheduler doesn't disable it
                task.update_active()
                TaskRunner.process_task(task)
            time.sleep(15)


class TaskRunner():

    handlers: dict[str, ThreadHandler] = {}
    task_timeout: int = 1800
    """
    Default task timeout in seconds, unless specified in the schedule config
    """

    @staticmethod
    def process_task(task: TaskItem):
        # Run in a second thread to tick over the active time
        # this is mostly here if something crashes and the
        # task never finishes, so we can check for stale active
        # times and deal with it accordingly
        running_dict = {}
        def _refresh_active(run: RunItem):
            try:
                while running_dict[run.run_idk]:
                    _update_run_times(run)
                    run.update_active()
                    time.sleep(30)
                # remove the run from the running dict to avoid
                # long running threads from taking up memory
                running_dict.pop(run.run_idk)
            except KeyError:
                # just handle any case where something else has removed the run
                # which means the run has finished/died
                pass

        def _update_run_times(run: RunItem):
            current_run_times = kvdb.get('current_run_times', list, 'local')
            if current_run_times is not None:
                new_output = {'run_times': current_run_times}
                run.set_output(new_output, merge=True)

        def _run_wrapper(run: RunItem):
            """
            Run wrapper to keep all of the 'same thread dependent'
            work (kvdb store mostly) in the same 'timeout thread'.
            """
            # Clear any existing runtimes in the current thread
            kvdb.store('local', 'current_run_times', [])
            # Set the run as started so when we update the active time it
            # has the version that has already started otherwise it will
            # set the active time on the unstarted version of the run
            run.set_running()
            # Run the function with the config provided in the run itself
            # this is to allow for manual runs to have different configs
            try:
                task.task_function(task, run, run.config)
            except Exception as e:
                run_function_store_exception(e)
            # When complete, also update run times
            _update_run_times(run)
            # if any of the current_run_times have a retry_count > 0 then set status as WARN
            #use run.output.get('run_times', [])
            if run.output is not None:
                for run_time in run.output.get('run_times', []):
                    if run_time['retry_count'] > 0:
                        run.set_warn({'message': f'Run {run.run_idk} had {run_time["retry_count"]} retries'})
                        return
            # only if it's still running do we want to set it as success,
            # otherwise it's already been set as failed, warn, etc and
            # we need to leave it in that state
            if run.status == tasks.RunStatus.RUNNING:
                run.set_success()

        # Because we have multiple schedules for a task we need
        # to get all the runs that are queued and run them because
        # some schedules will have runs queued at the same time
        queued_runs = task.get_queued_runs()
        # last_run = task.get_last_run()
        # if last_run is None or last_run.status != RunStatus.QUEUED:
        #     return
        # queued_runs = [last_run]
        for run in queued_runs:
            try:
                running_dict[run.run_idk] = True
                timeout = run.config.get('timeout', TaskRunner.task_timeout)
                ra_thread = threading.Thread(target=_refresh_active, args=(run,))
                ra_thread.start()
                run_function_with_timeout(
                    timeout=timeout,
                    message=f'Task {task.name} with run_id {run.run_idk} timed out (timeout: {timeout}s)',
                    func=_run_wrapper,
                    run=run
                )
                running_dict[run.run_idk] = False
            except Exception as e:
                run.set_failed(output={
                    'exception': str(e),
                })
                # if we have an exception the active time thread then just remove
                # the run from the running dict and let the active timer thread
                # catch the KeyError and stop itself
                running_dict.pop(run.run_idk)
                continue

    def __init__(
            self,
            run_in_thread = True,
            use_thread_groups = True,
            default_runner = True
        ):
        self.run_in_threads = run_in_thread
        self.use_thread_groups = use_thread_groups
        # If we have a dummy runner, then only register it as the default
        # if it's not already set. This is to allow for the dummy runner to
        # be set as the default runner in the tests or similar cases
        if default_runner:
            if tasks._register_task_with_runner is not None:
                raise Exception('Default task runner already set')
            else:
                tasks._register_task_with_runner = self.register_task

    def register_task(self, task: TaskItem):
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

        # Add task to the handler and will replace the task if it's already there
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
