from __future__ import absolute_import, print_function

from six.moves import range
import contextlib
import operator
import random
from functools import reduce

from pprint import pprint

import gevent
import gevent.pool
import mock

import ruskit.opsm as opsm

sleep_time_lb = 0.001
sleep_time_ub = 0.005
raise_msg = 'raise'
cleanup_msg = 'cleanup'
rterr = RuntimeError('RAISE EXCEPTION')


@contextlib.contextmanager
def global_echo_mock():
    global echo
    echo = mock.Mock()
    yield
    del echo


def typical_fail(task_name):
    return opsm.TaskFailure(
        task_name=task_name, error=rterr, grdst=None)


def typical_failclean(task_name, guard_name):
    return opsm.TaskFailure(
        task_name=task_name,
        error=rterr,
        grdst=opsm.TaskSuccess(
            task_name=guard_name, value=cleanup_msg))


def previous_fail(task_name):
    return opsm.TaskFailure(
        task_name=task_name,
        error=opsm.PreviousTaskFailedError(),
        grdst=None)


def assert_task_result(expect, actual):
    def assert_task_success(expect, actual):
        if expect.task_name != actual.task_name:
            return False

        if hasattr(expect.value, '__iter__'):
            if len(expect.value) != len(actual.value):
                return False
            return reduce(operator.and_, [
                assert_task_result_one(*pair)
                for pair in zip(expect.value, actual.value)
            ])
        else:
            return expect.value == actual.value

    def assert_task_failure(expect, actual):
        if expect.task_name != actual.task_name:
            return False

        check_error = True
        if hasattr(expect.error, '__iter__'):
            if len(expect.error) != len(actual.error):
                check_error = False
            check_error = reduce(operator.and_, [
                assert_task_result_one(*pair)
                for pair in zip(expect.error, actual.error)
            ])
        elif isinstance(expect.error, Exception):
            check_error = isinstance(actual, expect.__class__)
        else:
            check_error = expect.error == actual.error
        assert isinstance(actual.grdst, expect.grdst.__class__)
        if expect.grdst:
            return check_error and assert_task_result_one(
                expect.grdst, actual.grdst)
        else:
            return check_error

    def assert_task_result_one(expect, actual):
        _type_dispatch = {
            opsm.TaskSuccess: assert_task_success,
            opsm.TaskFailure: assert_task_failure,
        }
        if not isinstance(actual, expect.__class__):
            return False
        return _type_dispatch[expect.__class__](expect, actual)

    assert assert_task_result_one(expect, actual), '''Mismatch:
    Expect: {}
    Actual: {}'''.format(expect, actual)


class EchoTaskS(opsm.Task):
    def _setup(self, *args, **kwargs):
        self.msg = kwargs['msg']

    def _run(self):
        if self.msg == raise_msg:
            raise rterr
        else:
            echo(self.msg)
            return self.msg


class EchoTaskP(opsm.Task):
    def _setup(self, *args, **kwargs):
        self.msg = kwargs['msg']

    def _run(self):
        if self.msg == raise_msg:
            raise rterr
        else:
            gevent.sleep(random.uniform(sleep_time_lb, sleep_time_ub))
            echo(self.msg)
            return self.msg


class CleanupTask(opsm.Task):
    def _run(self):
        echo(cleanup_msg)
        return cleanup_msg


def test_task_success():
    with global_echo_mock():
        msg = 'hello'
        ee = EchoTaskS(msg=msg)
        ret = ee.run()

        echo.assert_called_once_with(msg)
        assert_task_result(
            opsm.TaskSuccess(
                task_name='EchoTaskS', value=msg), ret)


def test_task_failure():
    with global_echo_mock():
        ee = EchoTaskS(msg=raise_msg, guard=CleanupTask())
        ret = ee.run()

        assert_task_result(typical_failclean('EchoTaskS', 'CleanupTask'), ret)


def test_sequence_task_all_success():
    with global_echo_mock():
        num = 10

        ret_expect = opsm.TaskSuccess(
            task_name='SequenceTask',
            value=[
                opsm.TaskSuccess(
                    'EchoTaskS', value=i) for i in range(num)
            ])
        mock_call_expect = [mock.call(i) for i in range(num)]

        worker = opsm.SequenceTask(guard=CleanupTask())
        for i in range(num):
            worker.add(EchoTaskS(msg=i))
        ret = worker.run()

        assert mock_call_expect == echo.mock_calls
        assert_task_result(ret_expect, ret)


def test_sequence_task_partial_failure():
    with global_echo_mock():
        succ_num1 = 5
        fail_num1 = 3
        succ_num2 = 8

        # Expects
        ret_expect = []
        ret_expect += [
            opsm.TaskSuccess(
                task_name='EchoTaskS', value=i) for i in range(succ_num1)
        ]
        ret_expect += [typical_fail('EchoTaskS')]
        ret_expect += [
            previous_fail('EchoTaskS')
            for i in range(fail_num1 - 1 + succ_num2)
        ]
        ret_expect = opsm.TaskFailure(
            task_name='SequenceTask',
            error=ret_expect,
            grdst=opsm.TaskSuccess(
                task_name='CleanupTask', value=cleanup_msg))
        mock_calls_expect = [mock.call(i) for i in range(succ_num1)]
        mock_calls_expect += [mock.call('cleanup')]

        # Actuals
        worker = opsm.SequenceTask(guard=CleanupTask())
        for i in range(succ_num1):
            worker.add(EchoTaskS(msg=i))
        for _ in range(fail_num1):
            worker.add(EchoTaskS(msg=raise_msg))
        for i in range(succ_num2):
            worker.add(EchoTaskS(msg=i))
        ret = worker.run()

        assert mock_calls_expect == echo.mock_calls
        assert_task_result(ret_expect, ret)


def test_sequence_task_partial_failure_without_guard():
    with global_echo_mock():
        succ_num1 = 5
        fail_num1 = 3
        succ_num2 = 8

        # Expects
        ret_expect = []
        ret_expect += [
            opsm.TaskSuccess(
                task_name='EchoTaskS', value=i) for i in range(succ_num1)
        ]
        ret_expect += [typical_fail('EchoTaskS')]
        ret_expect += [
            previous_fail('EchoTaskS')
            for i in range(fail_num1 - 1 + succ_num2)
        ]
        ret_expect = opsm.TaskFailure(
            task_name='SequenceTask',
            error=ret_expect,
            grdst=None)
        mock_calls_expect = [mock.call(i) for i in range(succ_num1)]

        # Actuals
        worker = opsm.SequenceTask()
        for i in range(succ_num1):
            worker.add(EchoTaskS(msg=i))
        for _ in range(fail_num1):
            worker.add(EchoTaskS(msg=raise_msg))
        for i in range(succ_num2):
            worker.add(EchoTaskS(msg=i))
        ret = worker.run()

        assert mock_calls_expect == echo.mock_calls
        assert_task_result(ret_expect, ret)


def test_parallel_task_all_success():
    with global_echo_mock():
        num = 10
        thread_num = 3

        ret_expect = opsm.TaskSuccess(
            task_name='ParallelTask',
            value=[
                opsm.TaskSuccess(
                    task_name='EchoTaskP', value=i) for i in range(num)
            ])
        mock_call_expect = [mock.call(i) for i in range(num)]

        pool = gevent.pool.Pool(thread_num)
        worker = opsm.ParallelTask(pool, guard=CleanupTask())
        for i in range(num):
            worker.add(EchoTaskP(msg=i))
        ret = worker.run()

        assert sorted(mock_call_expect) == sorted(echo.mock_calls)
        assert_task_result(ret_expect, ret)


def test_parallel_task_partial_failure():
    with global_echo_mock():
        thread_num = 3
        succ_num1 = 5
        fail_num1 = 3
        succ_num2 = 8

        # Expects
        ret_expect = []
        ret_expect += [
            opsm.TaskSuccess(
                task_name='EchoTaskP', value=i) for i in range(succ_num1)
        ]
        ret_expect += [
            typical_fail('EchoTaskP')
            for i in range(fail_num1)
        ]
        ret_expect += [
            opsm.TaskSuccess(
                task_name='EchoTaskP', value=i) for i in range(succ_num2)
        ]
        ret_expect = opsm.TaskFailure(
            task_name='ParallelTask',
            error=ret_expect,
            grdst=opsm.TaskSuccess(
                task_name='CleanupTask', value=cleanup_msg))
        mock_calls_expect = [mock.call(i) for i in range(succ_num1)]
        mock_calls_expect += [mock.call(i) for i in range(succ_num2)]
        mock_calls_expect += [mock.call('cleanup')]

        # Actuals
        pool = gevent.pool.Pool(thread_num)
        worker = opsm.ParallelTask(pool, guard=CleanupTask())
        for i in range(succ_num1):
            worker.add(EchoTaskP(msg=i))
        for _ in range(fail_num1):
            worker.add(EchoTaskP(msg=raise_msg))
        for i in range(succ_num2):
            worker.add(EchoTaskP(msg=i))
        ret = worker.run()

        assert sorted(mock_calls_expect) == sorted(echo.mock_calls)
        assert_task_result(ret_expect, ret)


def test_parallel_task_partial_failure_without_guard():
    with global_echo_mock():
        thread_num = 3
        succ_num1 = 5
        fail_num1 = 3
        succ_num2 = 8

        # Expects
        ret_expect = []
        ret_expect += [
            opsm.TaskSuccess(
                task_name='EchoTaskP', value=i) for i in range(succ_num1)
        ]
        ret_expect += [typical_fail('EchoTaskP') for i in range(fail_num1)]
        ret_expect += [
            opsm.TaskSuccess(
                task_name='EchoTaskP', value=i) for i in range(succ_num2)
        ]
        ret_expect = opsm.TaskFailure(
            task_name='ParallelTask',
            error=ret_expect,
            grdst=None)
        mock_calls_expect = [mock.call(i) for i in range(succ_num1)]
        mock_calls_expect += [mock.call(i) for i in range(succ_num2)]

        # Actuals
        pool = gevent.pool.Pool(thread_num)
        worker = opsm.ParallelTask(pool)
        for i in range(succ_num1):
            worker.add(EchoTaskP(msg=i))
        for _ in range(fail_num1):
            worker.add(EchoTaskP(msg=raise_msg))
        for i in range(succ_num2):
            worker.add(EchoTaskP(msg=i))
        ret = worker.run()

        assert sorted(mock_calls_expect) == sorted(echo.mock_calls)
        assert_task_result(ret_expect, ret)


def test_complex_guard_successful():
    pass


def test_complex_guard_failed():
    pass
