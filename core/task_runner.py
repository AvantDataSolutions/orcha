
import time
import threading
import traceback

from orcha.core import tasks
from orcha.core.tasks import TaskItem, RunItem
from orcha.utils import kvdb
from orcha.utils import threading as orcha_threading
from orcha.utils.log import LogManager

# TODO terminate nicely
# https://itnext.io/containers-terminating-with-grace-d19e0ce34290
# https://docs.docker.com/engine/reference/commandline/stop/

BASE_THREAD_GROUP = 'base_thread'

runner_log = LogManager('task_runner')


class ThreadHandler():
    """
    The core class for handling threads in the task runner.
    This manages the actual running of tasks in threads and
    the updating of the active time for all tasks in the handler.
    """
    def __init__(self, thread_group: str):
        self.is_running = False
        self.thread_group = thread_group
        self.thread = None
        self.tasks: list[TaskItem] = []

    def start(self):
        """
        Start the thread handler. Will raise an exception if the
        thread is already running.
        """
        self.is_running = True
        if self.thread is not None:
            raise Exception('Thread already started')
        self.thread = threading.Thread(target=self._run, name=self.thread_group)
        self.thread.start()

    def stop(self):
        """
        Stop the thread handler. Will not raise an exception if the
        thread is not running.
        """
        self.is_running = False
        if self.thread is not None:
            self.thread.join()

    def add_task(self, task: TaskItem):
        """
        Add a task to the handler. If the task is already in the
        handler then it will be replaced by the new task.
        """
        # check if the task is in the list by id and if not append it
        task_ids = [t.task_idk for t in self.tasks]
        if task.task_idk not in task_ids:
            self.tasks.append(task)
        else:
            # replace the task if it's already in the list
            for i, t in enumerate(self.tasks):
                if t.task_idk == task.task_idk:
                    self.tasks[i] = task

    def update_active_all_tasks(self):
        """
        This sets all tasks as active in this thread group, as when one task
        is running, all tasks should be considered active as this thread
        will handle all of these tasks, even if its handling a long running task.
        """
        # Without this, if one task runs for 5 minutes, all other tasks will get
        # marked as inactive by the scheduler as they won't have been updated
        for task in self.tasks:
            task.update_active()

    def _run(self):
        """
        Main helper function for the ThreadHandler to run all tasks.
        This processes all tasks in the handler every 15 seconds and
        updates the active time for all tasks before each task is processed.
        """
        while self.is_running:
            # if an external process has updated the task then we need to reload it
            # otherwise we may be running an old version of the task which will at
            # the very least set the active time on the old version of the task
            all_tasks = TaskItem.get_all()
            tasks_dict = {task.task_idk: task for task in all_tasks}
            for task in self.tasks:
                db_task = tasks_dict.get(task.task_idk, None)
                if db_task and db_task.version != task.version:
                    # log that the task is being updated
                    runner_log.add_entry(
                        actor='main_loop', category='reloading_task',
                        text='Detected task version change, reloading task',
                        json={
                            'message': '',
                            'task': task.task_idk
                        }
                    )
                    # The database task doesn't have the task function itself
                    # so we need to copy it over from the current task
                    db_task.task_function = task.task_function
                    self.add_task(db_task)
            for task in self.tasks:
                # Update all tasks as active outside of processing the task
                # to make sure we get at least one guaranteed update
                self.update_active_all_tasks()
                self.process_task(task)
            time.sleep(15)

    def process_all_tasks(self):
        """
        Helper function to process all tasks in the handler
        """
        for task in self.tasks:
            self.process_task(task)

    def process_task(self, task: TaskItem):
        """
        Process a single task in the handler. This will run the task
        in a separate thread and update the active time for the task.
        """
        # log that the task is running
        runner_log.add_entry(
            actor='main_loop', category='processing_task',
            text='Processing task',
            json={'task': task.name}
        )
        # Run in a second thread to tick over the active time
        # this is mostly here if something crashes and the
        # task never finishes, so we can check for stale active
        # times and deal with it accordingly
        running_dict = {}
        def _run_progress_helper(run: RunItem):
            """
            This is a helper function to run in parallel with the run itself
            and perform non-blocking tasks such as:
            - Updating the active time of the task
            - Checking if the run has been cancelled
            - Updating the run times in the run output
            """
            while running_dict.get(run.run_idk, False):
                # sleep at the start so we will always get 'one last run'
                # before the loop is broken
                time.sleep(5)
                current_run_times = kvdb.get(
                    key='current_run_times',
                    as_type=list,
                    storage_type='local'
                )
                if current_run_times is not None and len(current_run_times) > 0:
                    new_output = {'run_times': current_run_times}
                    run.set_output(new_output, merge=True)

                run.update_active()
                self.update_active_all_tasks()
                # If the run has been cancelled then we need to stop the thread
                if run.status == 'cancelled':
                    orcha_threading.expire_timeout(
                        threading.current_thread().name
                    )
            # remove the run from the running dict to avoid
            # long running threads from taking up memory
            running_dict.pop(run.run_idk, None)

        def _run_wrapper(run: RunItem):
            """
            Run wrapper to keep all of the 'same thread dependent'
            work (kvdb store mostly) in the same 'timeout thread'.
            """
            # Clear any existing runtimes in the current thread
            kvdb.store(
                storage_type='local',
                key='current_run_times',
                value=[]
            )
            # Set the run as started so when we update the active time it
            # has the version that has already started otherwise it will
            # set the active time on the unstarted version of the run
            run.set_status('pending')
            run.set_progress('running')

            running_dict[run.run_idk] = True
            # We need to allow the _refresh_active function to use the
            # same store as the run itself to be able to read module
            # times while the run is in progress and update the output
            helper_thread = threading.Thread(
                target=_run_progress_helper,
                args=(run,),
                name=threading.current_thread().name
            )
            helper_thread.start()

            # We're using the threading exception store to store exceptions
            # to handle elsewhere
            try:
                # Run the function with the config provided in the run itself
                # this is to allow for manual runs to have different configs
                task.task_function(task, run, run.config)
            except Exception as e:
                orcha_threading.store_exception(e)

            running_dict[run.run_idk] = False
            # Join the helper thread to make sure it finishes
            helper_thread.join()

            # We'll often have version mismatch issues here
            # as the run has been updated in the helper thread
            run.reload()
            # if any of the current_run_times have a retry_count > 0 then set status as WARN
            if run.output is not None:
                for run_time in run.output.get('run_times', []):
                    if run_time['retry_count'] > 0:
                        message = f'Run {run.run_idk} had {run_time["retry_count"]} retries'
                        # Only set it as a warning if it hasn't failed already
                        # and explicitly set the retry_message not message to avoid overwriting
                        # any other failed messages
                        if run.status == 'failed' or run.status == 'cancelled':
                            run.set_output({'retry_message': message}, merge=True)
                        else:
                            run.set_status(status='warn', output={'message': message})

            # only if it's still running do we want to set it as success,
            # otherwise it's already been set as failed, warn, etc and
            # we need to leave it in that state
            thread_exception = orcha_threading.get_exception(
                threading.current_thread(),
                and_clear_exception=False
            )
            # If we had an exception in the thread then raise it
            if thread_exception:
                raise thread_exception

            # At this point we have to be running, if not then we have an issue
            if run.progress == 'running':
                # if nothing else has set the status then we can set it as success
                if run.status == 'pending':
                    run_s_set = task.get_schedule_from_id(run.set_idf)
                    # Check if we have any trigger tasks to run
                    if run_s_set and run_s_set.trigger_task:
                        trigger_task = run_s_set.trigger_task[0]
                        trigger_task_sset = run_s_set.trigger_task[1]
                        if not trigger_task_sset:
                            trigger_task_sset = trigger_task.schedule_sets[0]
                        new_run = trigger_task.trigger_run(
                            schedule=trigger_task_sset,
                            trigger_task=task,
                            scheduled_time=run.scheduled_time
                        )
                        if not new_run:
                            run.set_status('warn')
                            run.set_output(
                                {'message':'Trigger task failed to create run'},
                                merge=True
                            )
                    # The trigger task may have set the run as warn, which is ok
                    # and we can have the 'set success' call to quietly do nothing
                    run.set_status('success', raise_on_backwards=False)
            else:
                raise Exception(f'Run {run.run_idk} complete but not set run as running')

            # Once all updates are done, the run is complete
            run.set_progress('complete')

        # Because we have multiple schedules for a task we need
        # to get all the runs that are queued and run them because
        # some schedules will have runs queued at the same time
        queued_runs = task.get_queued_runs()
        for run in queued_runs:
            runner_log.add_entry(
                actor='main_loop', category='processing_run',
                text='Processing run',
                json={'task': task.name, 'run_id': run.run_idk}
            )
            try:
                use_timeouts = True
                if use_timeouts:
                    timeout = run.config.get('timeout', TaskRunner.task_timeout)
                    orcha_threading.run_function_with_timeout(
                        timeout=timeout,
                        message=f'Task {task.name} with run_id {run.run_idk} timed out (timeout: {timeout}s)',
                        thread_name=threading.current_thread().name,
                        func=_run_wrapper,
                        run=run,
                    )
                else:
                    _run_wrapper(run)
            except Exception as e:
                # Being safe and adding a reload to make sure we have the latest run
                run.reload()
                # The run may often be cancelled here so we don't want to raise
                # if we're going from cancelled to failed; leaving it as cancelled is ok
                run.set_status(
                    status='failed',
                    output={'exception':f'{str(e)}, \n' + f'{traceback.format_exc()}'},
                    raise_on_backwards=False
                )
                run.set_progress('complete')
                # if the run raised an exception then clean up the idk from the running_dict
                running_dict.pop(run.run_idk, None)
                continue


class TaskRunner():
    """
    The TaskRunner class is used to run tasks in threads and manage
    the running of tasks in the system.
    """
    handlers: dict[str, ThreadHandler] = {}
    task_timeout: int = 1800
    """
    Default task timeout in seconds, unless specified in the schedule config
    """

    def __init__(
            self,
            run_in_thread = True,
            use_thread_groups = True,
            default_runner = True
        ):
        runner_log.add_entry(
            actor='task_runner', category='setup', text='Setting up task runner',
            json={'run_in_thread': run_in_thread, 'use_thread_groups': use_thread_groups}
        )
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
        """
        This registers a task with the task runner such that the runner
        will run the task when it finds queued runs for the task.
        """
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

        runner_log.add_entry(
            actor='task_runner', category='registering_task',
            text='Registering task',
            json={'task': task.name, 'thread_group': thread_group}
        )

        # Add task to the handler and will replace the task if it's already there
        self.handlers[thread_group].add_task(task)

    def register_tasks(self, tasks: list[TaskItem]):
        """
        This registers a list of tasks with the task runner.
        Simple wrapper around the register_task function.
        """
        for task in tasks:
            self.register_task(task)

    def process_all_tasks(self):
        """
        This processes all tasks in all the handlers.
        """
        for handler in self.handlers.values():
            handler.process_all_tasks()

    def stop_all(self, stop_base = False):
        """
        This stops all the threads that have been registered.
        """
        for handler in self.handlers.values():
            if handler.thread_group == BASE_THREAD_GROUP and not stop_base:
                continue
            handler.stop()

    def start_all(self, start_base = True):
        """
        This starts all the threads that have been registered
        and restarts any that are not alive.
        """
        for handler in self.handlers.values():
            if handler.thread_group == BASE_THREAD_GROUP and not start_base:
                continue
            if handler.thread is None or not handler.thread.is_alive():
                handler.start()

    def all_alive(self):
        """
        Returns True if all the threads are alive or returns the
        thread groups that are not alive.
        """
        alive = True
        not_alive = []
        for handler in self.handlers.values():
            if handler.thread is None or not handler.thread.is_alive():
                not_alive.append(handler.thread_group)
                alive = False
        if not alive:
            return not_alive
        return alive