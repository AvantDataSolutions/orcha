import os
import unittest
from datetime import datetime as dt

from orcha.core import initialise, tasks

orcha_user = os.getenv('ORCHA_CORE_USER', '')
orcha_passwd = os.getenv('ORCHA_CORE_PASSWORD', '')
orcha_server = os.getenv('ORCHA_CORE_SERVER', '')
orcha_db = os.getenv('ORCHA_CORE_DB', '')

assert orcha_user != ''
assert orcha_passwd != ''
assert orcha_server != ''
assert orcha_db != ''


def create_test_task(
        task_number: str,
        func = lambda task_item, run_item, unknown_dict: None,
        s_sets = [tasks.ScheduleSet('* * * * *', {})],
        thread_group = 'test_thread_group'
    ):
    task = tasks.TaskItem.create(
        task_idk=f'test_task_{task_number}',
        name=f'Test Task {task_number}',
        description=f'A test task for task {task_number}',
        task_function=func,
        schedule_sets=s_sets,
        thread_group=thread_group
    )
    return task


class a_UninitalisedTests(unittest.TestCase):

    # correctly make sure we don't start initialised
    def test_a_001_check_uninitialised(self):
        with self.assertRaises(RuntimeError):
            tasks.confirm_initialised()


    # can't get anything if we're not initialised
    def test_a_002_task_get_all(self):
        with self.assertRaises(RuntimeError):
            tasks.TaskItem.get_all()


    def test_a_003_task_get_one(self):
        with self.assertRaises(RuntimeError):
            tasks.TaskItem.get('not_a_task')


    def test_a_004_task_create(self):
        with self.assertRaises(RuntimeError):
            create_test_task('1')


    def test_a_005_run_get_all(self):
        with self.assertRaises(RuntimeError):
            tasks.RunItem.get_all(task_id='', schedule=None, since=dt.utcnow())


    def test_a_006_run_get_one(self):
        with self.assertRaises(RuntimeError):
            tasks.RunItem.get('not_a_run')


    def test_a_007_run_get_all_queued(self):
        with self.assertRaises(RuntimeError):
            tasks.RunItem.get_all_queued(task_id='', schedule=None)


    # make sure get_running_runs doesn't work
    def test_a_008_run_get_running(self):
        with self.assertRaises(RuntimeError):
            tasks.RunItem.get_running_runs(task_id='', schedule=None)


    # initialise the database which shouldn't raise an exception
    def test_a_009_check_initialisation(self):
        initialise(
            orcha_user=orcha_user,
            orcha_pass=orcha_passwd,
            orcha_server=orcha_server,
            orcha_db=orcha_db
        )
        tasks.confirm_initialised()

    # test logic to make sure a task runner is required before creating a task
    def test_a_010_task_runner_required(self):
        with self.assertRaises(Exception):
            create_test_task('1')

