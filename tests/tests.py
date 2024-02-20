import os
import time
import unittest
from datetime import datetime as dt
from datetime import timedelta as td

import pandas as pd
import sqlalchemy

from orcha.core import initialise, scheduler, task_runner, tasks
from orcha.core.module_base import PythonEntity, PythonSource

orcha_user = os.getenv('ORCHA_CORE_USER', '')
orcha_passwd = os.getenv('ORCHA_CORE_PASSWORD', '')
orcha_server = os.getenv('ORCHA_CORE_SERVER', '')
orcha_db = os.getenv('ORCHA_CORE_DB', '')

assert orcha_user != ''
assert orcha_passwd != ''
assert orcha_server != ''
assert orcha_db != ''

def empty_database():
    # connect to the database and delete all the tables
    engine = sqlalchemy.create_engine(f'postgresql://{orcha_user}:{orcha_passwd}@{orcha_server}/{orcha_db}')
    connection = engine.connect()
    connection.execute(sqlalchemy.text('''
        DO $$
        BEGIN
            IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'orcha' AND table_name = 'tasks') THEN
                EXECUTE 'TRUNCATE TABLE orcha.tasks';
            END IF;
            IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'orcha' AND table_name = 'runs') THEN
                EXECUTE 'TRUNCATE TABLE orcha.runs';
            END IF;
            IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'orcha_logs' AND table_name = 'logs') THEN
                EXECUTE 'TRUNCATE TABLE orcha_logs.logs';
            END IF;
        END $$;
    '''))
    connection.commit()
    connection.close()


def create_test_task(
        task_number: str,
        func = lambda task_item, run_item, unknown_dict: None,
        s_sets = [tasks.ScheduleSet('* * * * *', {})],
        thread_group = 'test_thread_group',
        register_with_runner = False
    ):
    task = tasks.TaskItem.create(
        task_idk=f'test_task_{task_number}',
        name=f'Test Task {task_number}',
        description=f'A test task for task {task_number}',
        task_function=func,
        schedule_sets=s_sets,
        thread_group=thread_group,
        register_with_runner=register_with_runner
    )
    return task

# initialise the orcha core so the tests can run
initialise(
    orcha_user=orcha_user,
    orcha_pass=orcha_passwd,
    orcha_server=orcha_server,
    orcha_db=orcha_db
)
tasks.confirm_initialised()

# create a task runner and a scheduler as we only have one of these per
# instance/container/system
runner = task_runner.TaskRunner()
sched = scheduler.Scheduler(
    config=scheduler.OrchaSchedulerConfig(
        prune_interval=10,
        task_refresh_interval=2
    )
)

class a_TaskManagement(unittest.TestCase):
    def setUp(self):
        empty_database()
        sched.pause()


    # make sure we have no tasks in the database
    def test_a_002_check_no_tasks_exist(self):
        all_tasks = tasks.TaskItem.get_all()
        self.assertEqual(len(all_tasks), 0)


    # create a task runner and then create a task
    def test_a_003_create_task(self):
        task_uuid = 'a_003_test_task'
        task_id = f'test_task_{task_uuid}'
        create_test_task(task_uuid)
        task = tasks.TaskItem.get(task_id)
        self.assertIsNotNone(task)

        all_tasks = tasks.TaskItem.get_all()
        self.assertEqual(len(all_tasks), 1)
        self.assertEqual(all_tasks[0].task_idk, task_id)

        task_uuid_2 = 'a_003_test_task_2'
        create_test_task(task_uuid_2)
        all_tasks = tasks.TaskItem.get_all()
        self.assertEqual(len(all_tasks), 2)


    # test the ability to change task statuses
    def test_a_006_task_status(self):
        task_uuid = 'a_006_test_task'
        task_id = f'test_task_{task_uuid}'
        task = create_test_task(task_uuid)
        task = tasks.TaskItem.get(task_id)
        self.assertIsNotNone(task)
        assert task is not None
        statuses = ['enabled', 'disabled', 'inactive', 'deleted']
        for status in statuses:
            task.set_status(status, 'test status change') # type: ignore
            self.assertEqual(task.status, status)
            # get the task again to make sure the status change was saved
            task = tasks.TaskItem.get(task_id)
            assert task is not None
            self.assertEqual(task.status, status)


    # test when a run is due for the task
    def test_a_007_run_due(self):
        task_uuid = 'a_007_test_task'
        task_id = f'test_task_{task_uuid}'
        task = create_test_task(task_uuid)
        task = tasks.TaskItem.get(task_id)
        assert task is not None
        # we dont have any runs, so it must be due
        is_due, last_run = task.is_run_due_with_last(schedule=task.schedule_sets[0])
        self.assertTrue(is_due)
        self.assertIsNone(last_run)


    # create a run and check that it's not due
    def test_a_008_run_not_due(self):
        task_uuid = 'a_008_test_task'
        task_id = f'test_task_{task_uuid}'
        task = create_test_task(task_uuid)
        task = tasks.TaskItem.get(task_id)
        assert task is not None
        run = task.schedule_run(schedule=task.schedule_sets[0])
        is_due, last_run = task.is_run_due_with_last(schedule=task.schedule_sets[0])
        self.assertFalse(is_due)
        self.assertIsNotNone(last_run)
        assert last_run is not None
        self.assertEqual(run.run_idk, last_run.run_idk)
        self.assertEqual(run.status, 'queued')

        task.set_status('disabled', 'test status change')


class b_RunManagement(unittest.TestCase):
    def setUp(self):
        empty_database()

    def test_b_001_simple_create_run(self):
        task_uuid = 'b_001_test_task'
        task = create_test_task(task_uuid)
        self.assertIsNotNone(task)
        assert task is not None

        run = task.schedule_run(schedule=task.schedule_sets[0])
        is_due, last_run = task.is_run_due_with_last(schedule=task.schedule_sets[0])
        self.assertFalse(is_due)
        self.assertIsNotNone(last_run)
        assert last_run is not None
        self.assertEqual(run.run_idk, last_run.run_idk)
        self.assertEqual(run.status, 'queued')


    # prune all runs for test_task
    def test_b_003_prune_runs(self):
        task_uuid = 'b_003_test_task'
        task_id = f'test_task_{task_uuid}'
        task = create_test_task(task_uuid)
        assert task is not None

        task.prune_runs(max_age=td(seconds=0))
        all_runs = tasks.RunItem.get_all(
            task=task_id,
            schedule=task.schedule_sets[0],
            since=dt.utcnow() - td(days=1)
        )
        self.assertEqual(len(all_runs), 0)
        # make sure a run is now due
        is_due, last_run = task.is_run_due_with_last(schedule=task.schedule_sets[0])
        self.assertTrue(is_due)
        self.assertIsNone(last_run)

        run_1 = task.schedule_run(schedule=task.schedule_sets[0])
        run_2 = task.schedule_run(schedule=task.schedule_sets[0])

        all_runs = tasks.RunItem.get_all(
            task=task_id,
            schedule=task.schedule_sets[0],
            since=dt.utcnow() - td(days=1)
        )
        self.assertEqual(len(all_runs), 2)
        created_run_ids = [run_1.run_idk, run_2.run_idk]
        for run in all_runs:
            self.assertIn(run.run_idk, created_run_ids)

        # make sure the schedules they were created for are correct
        for run in all_runs:
            self.assertEqual(run.set_idf, task.schedule_sets[0].set_idk)

        # make sure the statuses are all correct
        for run in all_runs:
            self.assertEqual(run.status, 'queued')

        task.set_status('disabled', 'test status change')


    # prune all runs for test_task
    def test_b_004_update_run_status(self):
        task_uuid = 'b_004_test_task'
        task = create_test_task(task_uuid)
        assert task is not None

        run = task.schedule_run(schedule=task.schedule_sets[0])

        # make sure the run is queued
        self.assertEqual(run.status, 'queued')
        # make the run running
        run.set_running(output={'test': 'output'})
        self.assertEqual(run.status, 'running')
        # make sure the output is right
        self.assertEqual(run.output, {'test': 'output'})
        # make the run completed
        run.set_success(output={'test': 'success'})
        self.assertEqual(run.status, 'success')
        run.set_warn(output={'test': 'warn'})
        self.assertEqual(run.status, 'warn')
        run.set_failed(output={'test': 'failed'})
        self.assertEqual(run.status, 'failed')
        # now try and do things that will raise exceptions
        with self.assertRaises(Exception):
            run.set_running(output={'test': 'output'})
        with self.assertRaises(Exception):
            run.set_success(output={'test': 'success'})
        with self.assertRaises(Exception):
            run.set_warn(output={'test': 'warn'})

        task.set_status('disabled', 'test status change')


    # make sure the runs are associated with the correct tasks and scheduled
    def test_b_005_run_association(self):
        task_uuid = 'b_005_test_task'
        task_id = f'test_task_{task_uuid}'
        task_uuid_2 = 'b_005_test_task_2'
        task_id_2 = f'test_task_{task_uuid_2}'
        s_set_1min = tasks.ScheduleSet('* * * * *', {'test': '1min'})
        s_set_5min = tasks.ScheduleSet('*/5 * * * *', {'test': '5min'})
        task_1 = create_test_task(
            task_number=task_uuid,
            s_sets=[s_set_1min, s_set_5min]
        )
        task_2 = create_test_task(
            task_number=task_uuid_2,
            s_sets=[s_set_5min]
        )
        assert task_1 is not None
        assert task_2 is not None

        t1_1min_sset = tasks.ScheduleSet.create_with_key(
            set_idk=f'{task_1.task_idk}_{s_set_1min.cron_schedule}',
            cron_schedule=s_set_1min.cron_schedule,
            config=s_set_1min.config
        )

        t1_5min_sset = tasks.ScheduleSet.create_with_key(
            set_idk=f'{task_1.task_idk}_{s_set_5min.cron_schedule}',
            cron_schedule=s_set_5min.cron_schedule,
            config=s_set_5min.config
        )

        t2_5min_sset = tasks.ScheduleSet.create_with_key(
            set_idk=f'{task_2.task_idk}_{s_set_5min.cron_schedule}',
            cron_schedule=s_set_5min.cron_schedule,
            config=s_set_5min.config
        )

        run_t1_1min = task_1.schedule_run(schedule=t1_1min_sset)
        run_t1_5min = task_1.schedule_run(schedule=t1_5min_sset)
        run_t2_5min = task_2.schedule_run(schedule=t2_5min_sset)

        runs_t1_1min = tasks.RunItem.get_all(
            task=task_id,
            schedule=t1_1min_sset,
            since=dt.utcnow() - td(days=1)
        )
        self.assertEqual(len(runs_t1_1min), 1)
        self.assertEqual(runs_t1_1min[0].run_idk, run_t1_1min.run_idk)

        runs_t1_5min = tasks.RunItem.get_all(
            task=task_id,
            schedule=t1_5min_sset,
            since=dt.utcnow() - td(days=1)
        )
        self.assertEqual(len(runs_t1_5min), 1)
        self.assertEqual(runs_t1_5min[0].run_idk, run_t1_5min.run_idk)

        runs_t2_5min = tasks.RunItem.get_all(
            task=task_id_2,
            schedule=t2_5min_sset,
            since=dt.utcnow() - td(days=1)
        )
        self.assertEqual(len(runs_t2_5min), 1)
        self.assertEqual(runs_t2_5min[0].run_idk, run_t2_5min.run_idk)

        runs_t1_all = tasks.RunItem.get_all(
            task=task_id,
            since=dt.utcnow() - td(days=1)
        )

        self.assertEqual(len(runs_t1_all), 2)
        run_ids = [run_t1_1min.run_idk, run_t1_5min.run_idk]
        for run in runs_t1_all:
            self.assertIn(run.run_idk, run_ids)

        task_1.set_status('disabled', 'test status change')
        task_2.set_status('disabled', 'test status change')


    # make sure get queued and running runs works correctly
    def test_b_006_get_queued_running(self):
        empty_database()
        task_uuid = 'b_006_test_task'
        task_id = f'test_task_{task_uuid}'
        task_1 = create_test_task(task_uuid)
        assert task_1 is not None

        run_t1_1min = task_1.schedule_run(schedule=task_1.schedule_sets[0])

        queued_runs = tasks.RunItem.get_all_queued(task=task_id)
        self.assertEqual(len(queued_runs), 1)
        self.assertEqual(queued_runs[0].run_idk, run_t1_1min.run_idk)

        running_runs = tasks.RunItem.get_running_runs(task=task_id)
        self.assertEqual(len(running_runs), 0)

        run_t1_1min.set_running(output={'test': 'output'})
        running_runs = tasks.RunItem.get_running_runs(task=task_id)
        self.assertEqual(len(running_runs), 1)
        self.assertEqual(running_runs[0].run_idk, run_t1_1min.run_idk)
        self.assertEqual(running_runs[0].output, {'test': 'output'})

        run_t1_1min.set_success(output={'test': 'success'})
        running_runs = tasks.RunItem.get_running_runs(task=task_id)
        self.assertEqual(len(running_runs), 0)

        queued_runs = tasks.RunItem.get_all_queued(task=task_id)
        self.assertEqual(len(queued_runs), 0)


class c_SchedulerAndRunnerTests(unittest.TestCase):
    def setUp(self):
        empty_database()
        sched.start()

    # test the scheduler
    def test_c_001_simple_queuing(self):
        test_start_time = dt.utcnow()
        empty_database()
        sched.start()
        s_set_1min = tasks.ScheduleSet('* * * * *', {'test': '1min'})
        s_set_5min = tasks.ScheduleSet('*/5 * * * *', {'test': '5min'})

        task_uuid = 'c_001_test_task'
        task_id = f'test_task_{task_uuid}'

        task_uuid_2 = 'c_001_test_task_2'
        task_id_2 = f'test_task_{task_uuid_2}'

        task_1 = create_test_task(
            task_number=task_uuid,
            s_sets=[s_set_1min, s_set_5min],
            func=lambda task_item, run_item, unknown_dict: run_item.set_output({'data': 'test_output'}),
            register_with_runner=True
        )
        task_2 = create_test_task(
            task_number=task_uuid_2,
            s_sets=[s_set_5min],
            register_with_runner=True
        )
        assert task_1 is not None
        assert task_2 is not None

        # this is using a schedule set that doesn't have an idk
        # and isn't assigned to the task, so it should raise an exception
        with self.assertRaises(Exception):
            t1_runs = tasks.RunItem.get_all(
                task=task_id,
                schedule=s_set_1min,
                since=dt.utcnow() - td(days=1)
            )

        # make sure we have no runs
        t1_runs = tasks.RunItem.get_all(
            task=task_id,
            since=dt.utcnow() - td(days=1)
        )
        self.assertEqual(len(t1_runs), 0)

        t2_runs = tasks.RunItem.get_all(
            task=task_id_2,
            since=dt.utcnow() - td(days=1)
        )
        self.assertEqual(len(t2_runs), 0)

        # sleep for 15 seconds to make sure the scheduler has time to run
        time.sleep(15)

        # make sure the runs are associated with the correct tasks and scheduled
        runs_t1_1min = tasks.RunItem.get_all(
            task=task_id,
            schedule=task_1.schedule_sets[0],
            since=dt.utcnow() - td(days=1)
        )
        self.assertEqual(len(runs_t1_1min), 1)

        runs_t1_5min = tasks.RunItem.get_all(
            task=task_id,
            schedule=task_1.schedule_sets[1],
            since=dt.utcnow() - td(days=1)
        )
        self.assertEqual(len(runs_t1_5min), 1)

        runs_t2_5min = tasks.RunItem.get_all(
            task=task_id_2,
            schedule=task_2.schedule_sets[0],
            since=dt.utcnow() - td(days=1)
        )
        self.assertEqual(len(runs_t2_5min), 1)

        # sleep 20 seconds to make sure the runs have time
        # to be picked up by the runner and completed
        time.sleep(20)

        runs_t1_1min = tasks.RunItem.get_all(
            task=task_id,
            schedule=task_1.schedule_sets[0],
            since=dt.utcnow() - td(days=1)
        )
        # We should still have 1 run, and it should be done, and should have
        # the output we set in the task
        self.assertEqual(len(runs_t1_1min), 1)
        self.assertEqual(runs_t1_1min[0].status, 'success')
        # run times should be correctly appended, and also empty as we have no module
        # in this case
        self.assertEqual(runs_t1_1min[0].output, {'data': 'test_output', 'run_times': []})

        # sleep the rest of the time until 1 minute has elapsed
        time.sleep(60 - (dt.utcnow() - test_start_time).total_seconds())
        # sleep another 30 seconds to let another minutely run be created and completed
        time.sleep(30)

        runs_t1_1min = tasks.RunItem.get_all(
            task=task_id,
            schedule=task_1.schedule_sets[0],
            since=dt.utcnow() - td(days=1)
        )
        # We should now have 2 runs and both should be done
        self.assertEqual(len(runs_t1_1min), 2)
        self.assertEqual(runs_t1_1min[0].status, 'success')
        self.assertEqual(runs_t1_1min[1].status, 'success')

        sched.pause()


    # test some runs that will warn and fail
    def test_c_002_runs_warn_fail(self):
        empty_database()
        sched.start()
        s_set_1min = tasks.ScheduleSet('* * * * *', {'test': '1min'})

        def warn_function(task_item, run_item, unknown_dict):
            run_item.set_warn({'message': 'test warning'})

        def fail_function(task_item, run_item, unknown_dict):
            run_item.set_failed({'message': 'test failure'})

        task_uuid_warn = 'c_002_test_task_warn'
        task_uuid_fail = 'c_002_test_task_fail'
        task_id_warn = f'test_task_{task_uuid_warn}'
        task_id_fail = f'test_task_{task_uuid_fail}'


        task_warn = tasks.TaskItem.create(
            task_idk=task_id_warn,
            name='Test Task Warn',
            description='A test task for task warn',
            task_function=warn_function,
            schedule_sets=[s_set_1min],
            thread_group='test_thread_group',
        )

        task_fail = tasks.TaskItem.create(
            task_idk=task_id_fail,
            name='Test Task Fail',
            description='A test task for task fail',
            task_function=fail_function,
            schedule_sets=[s_set_1min],
            thread_group='test_thread_group'
        )

        # sleep for 20 seconds to allow a run to be created
        time.sleep(20)
        # get the runs and make sure they're warn/fail as expected
        runs_warn = tasks.RunItem.get_all(
            task=task_id_warn,
            schedule=task_warn.schedule_sets[0],
            since=dt.utcnow() - td(days=1)
        )
        self.assertEqual(len(runs_warn), 1)

        runs_fail = tasks.RunItem.get_all(
            task=task_id_fail,
            schedule=task_fail.schedule_sets[0],
            since=dt.utcnow() - td(days=1)
        )
        self.assertEqual(len(runs_fail), 1)

        # sleep for another 20 seconds to allow the runs to be picked up and completed
        time.sleep(20)

        # then get those runs again and make sure they're warn/fail as expected
        run_warn = tasks.RunItem.get(
            run_id=runs_warn[0].run_idk,
            task=task_warn
        )
        assert run_warn is not None

        run_fail = tasks.RunItem.get(
            run_id=runs_fail[0].run_idk,
            task=task_fail
        )
        assert run_fail is not None


        self.assertEqual(run_warn.status, 'warn')
        self.assertEqual(run_warn.output, {'message': 'test warning', 'run_times': []})

        self.assertEqual(run_fail.status, 'failed')
        self.assertEqual(run_fail.output, {'message': 'test failure', 'run_times': []})

        sched.pause()


    # test the scheduler for expiry and pruning
    def test_c_003_expiry_and_pruning(self):
        empty_database()

        task_uuid_old = 'c_003_test_task_old'

        task_old = create_test_task(task_uuid_old)

        # create a run that is 2 minutes old
        run_old = task_old.schedule_run(schedule=task_old.schedule_sets[0])
        run_old.scheduled_time = dt.utcnow() - td(minutes=2)
        # force a db update, not normally done
        run_old._update_db()

        run_to_be_pruned = task_old.schedule_run(schedule=task_old.schedule_sets[0])
        run_to_be_pruned.scheduled_time = dt.utcnow() - td(days=1000)
        # force a db update, not normally done
        run_to_be_pruned._update_db()

        # sleep for 30 seconds to allow the run to be picked up
        time.sleep(30)

        # get the run and make sure it's done
        run_old = tasks.RunItem.get(
            run_id=run_old.run_idk,
            task=task_old
        )
        assert run_old is not None

        self.assertEqual(run_old.status, 'failed')
        assert run_old.output is not None
        self.assertEqual(run_old.output['message'], 'Run failed to start')

        # make sure the run to be pruned is pruned
        run_to_be_pruned = tasks.RunItem.get(
            run_id=run_to_be_pruned.run_idk,
            task=task_old
        )
        self.assertIsNone(run_to_be_pruned)

        sched.pause()


    # create a module and make sure the outputs are correct
    def test_c_004_module_tests(self):
        empty_database()
        sched.pause()
        entity = PythonEntity(
            module_idk='test_entity',
            description='A test entity',
            user_name='test_user',
            password='test_password',
        )

        source = PythonSource(
            module_idk='test_source',
            description='A test source',
            data_entity=entity,
            function=lambda x: pd.DataFrame({'test': [1, 2, 3]})
        )


        def module_function(task_item, run_item: tasks.RunItem, unknown_dict):
            data = source.get()
            run_item.set_output(output={'data': data.to_dict(orient='records')})

        task_uuid = 'c_004_test_task'
        task_id = f'test_task_{task_uuid}'

        task = create_test_task(
            task_number=task_uuid,
            func=module_function,
            register_with_runner=True
        )

        # testing the module, no need for the scheduler to create the run
        task.schedule_run(schedule=task.schedule_sets[0])

        # sleep for 30 seconds to allow the run to be picked up
        time.sleep(30)

        run = tasks.RunItem.get_all(
            task=task_id,
            schedule=task.schedule_sets[0],
            since=dt.utcnow() - td(days=1)
        )[0]

        self.assertEqual(run.status, 'success')
        assert run.output is not None
        self.assertEqual(run.output['data'], [{'test': 1}, {'test': 2}, {'test': 3}])
        # make sure the runtimes are correct
        self.assertEqual(len(run.output['run_times']), 1)
        # make sure all of the keys are in the run_times dict
        run_time = run.output['run_times'][0]
        self.assertIn('module_idk', run_time)
        self.assertIn('start_time_posix', run_time)
        self.assertIn('end_time_posix', run_time)
        self.assertIn('duration_seconds', run_time)

        # make sure the duration is less than 1 second
        self.assertLess(run_time['duration_seconds'], 1)

        # test the sleep source
        sleep_source = PythonSource(
            module_idk='test_sleep_source',
            description='A test source',
            data_entity=entity,
            function=lambda x: (time.sleep(5), pd.DataFrame())[1]
        )

        def sleep_function(task_item, run_item: tasks.RunItem, unknown_dict):
            data = sleep_source.get()
            run_item.set_output(output={'data': data.to_dict(orient='records')})

        task_uuid_2 = 'c_004_test_task_2'
        task_id_2 = f'test_task_{task_uuid_2}'

        sleep_task = create_test_task(
            task_number=task_uuid_2,
            func=sleep_function,
            register_with_runner=True
        )

        # testing the module, no need for the scheduler to create the run
        sleep_task.schedule_run(schedule=sleep_task.schedule_sets[0])

        # sleep for 20 seconds to allow the run to be picked up
        time.sleep(20)

        run = tasks.RunItem.get_all(
            task=task_id_2,
            schedule=sleep_task.schedule_sets[0],
            since=dt.utcnow() - td(days=1)
        )[0]

        self.assertEqual(run.status, 'success')
        assert run.output is not None
        self.assertEqual(run.output['data'], [])
        # make sure the runtimes are correct
        self.assertEqual(len(run.output['run_times']), 1)
        # and make sure it took roughly 5 seconds
        self.assertLess(run.output['run_times'][0]['duration_seconds'], 6)
        self.assertGreater(run.output['run_times'][0]['duration_seconds'], 4)
