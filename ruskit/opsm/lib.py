# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

from collections import namedtuple
import sys

from . import exceptions
from . import utils


class TaskSuccess(namedtuple('TaskSuccess', 'task_name, value')):
    def __py2_str__(self):
        ret = u'{}({}) ✓'.encode('utf8')
        ret = ret.format(self.task_name, self.value)
        return ret

    def __py3_str__(self):
        return '{}({}) ✓'.format(self.task_name, self.value)

    def __py2_repr__(self):
        ret = u'{} ✓'.encode('utf8')
        ret = ret.format(self.task_name)
        return ret

    def __py3_repr__(self):
        return '{} ✓'.format(self.task_name)

    def __str__(self):
        if sys.version_info.major < 3:
            return self.__py2_str__()
        else:
            return self.__py3_str__()

    def __repr__(self):
        if sys.version_info.major < 3:
            return self.__py2_repr__()
        else:
            return self.__py3_repr__()

    def ok(self):
        return True

    def val(self):
        return self.value

    def err(self):
        return None

    def unwrap(self):
        return self.value

    def aggregate(self):
        if utils.is_iterable_not_str(self.value):
            return tuple(v.aggregate() for v in self.value)
        elif isinstance(self.value, tuple):
            return self.value
        else:
            return (self.value,)


class TaskFailure(namedtuple('TaskFailure', 'task_name, error, grdst')):
    def __py2_str__(self):
        if self.grdst:
            ret = u'{}({}) ✗ => {}'.encode('utf8')
            ret = ret.format(self.task_name, self.error, self.grdst)
        else:
            ret = u'{}({}) ✗'.encode('utf8')
            ret = ret.format(self.task_name, self.error)
        return ret

    def __py3_str__(self):
        if self.grdst:
            ret = '{}({}) ✗ => {}'.format(self.task_name, self.error,
                                          self.grdst)
        else:
            ret = '{}({}) ✗'.format(self.task_name, self.error)
        return ret

    def __py2_repr__(self):
        if self.grdst:
            ret = u'{} ✗ => {}'.encode('utf8')
            ret = ret.format(self.task_name, self.grdst.task_name)
        else:
            ret = u'{} ✗'.encode('utf8')
            ret = ret.format(self.task_name)
        return ret

    def __py3_repr__(self):
        if self.grdst:
            ret = '{} ✗ => {}'.format(self.task_name, self.grdst.task_name)
        else:
            ret = '{} ✗'.format(self.task_name)
        return ret

    def __str__(self):
        if sys.version_info.major < 3:
            return self.__py2_str__()
        else:
            return self.__py3_str__()

    def __repr__(self):
        if sys.version_info.major < 3:
            return self.__py2_repr__()
        else:
            return self.__py3_repr__()

    def ok(self):
        return False

    def val(self):
        return None

    def err(self):
        return self.error

    def unwrap(self):
        raise exceptions.OPSMReturnOnErrorShortcutException()

    def aggregate(self):
        raise exceptions.OPSMReturnOnErrorShortcutException()


class Task(object):
    '''
    A Task is a base abstract object for implementing task-based
    event scheduling.

    Anyone who'd like to create a task should follow the example:

    @example:
        class EchoTask(Task):
            def _setup(self, *args, **kwargs):
                self.msg = kwargs.get('msg')
            def _run(self):
                if not self.msg:
                    raise ValueError('No message found')
                print(self.msg)

    the `setup` method is optional, while the run method should
    be implemented.

    A Guard is a `Task` used to guard of
    If you have specified any `guard`, raising any exception
    or setting `self.ok = False` will trigger the `guard`.
    '''

    def __init__(self, *args, **kwargs):
        self.ok = True
        self._task_name = self.__class__.__name__
        self.guard = kwargs.get('guard')
        self._setup(*args, **kwargs)

    def _setup(self, *args, **kwargs):
        '''
        Optional for derived classes
        '''
        pass

    def _try_guard(self):
        try:
            if self.guard:
                ret = self.guard.run()
            else:
                ret = None
        except Exception as e:
            ret = TaskFailure(self._task_name, error=e, grdst=None)
        return ret

    def run(self):
        try:
            rslt = self._run()
            ret = TaskSuccess(self._task_name, value=rslt)
        except Exception as e:
            ret = TaskFailure(self._task_name, error=e, grdst=None)
            self.ok = False
        finally:
            if self.ok:
                return ret
            else:
                grdst = self._try_guard()
                return TaskFailure(
                    self._task_name, error=ret.error, grdst=grdst)

    def _run(self):
        raise NotImplementedError("Should override _run")


class SequenceTask(Task):
    def __init__(self, *tasks, **kwargs):
        super(SequenceTask, self).__init__(**kwargs)

        self.subtasks = list(tasks)

    def add(self, task):
        self.subtasks.append(task)

    def run(self):
        return self._run()

    def _run_one(self, task):
        if self.ok is True:
            ret = None
            try:
                ret = task.run()
            except Exception as e:
                ret = TaskFailure(task._task_name, error=e, grdst=None)
            if not ret.ok():
                self.ok = False
            return ret
        else:
            return TaskFailure(
                task._task_name,
                error=exceptions.PreviousTaskFailedError(),
                grdst=None)

    def _run(self):
        ret = [self._run_one(task) for task in self.subtasks]
        if self.ok is True:
            return TaskSuccess(self._task_name, value=ret)
        else:
            grdst = self._try_guard()
            return TaskFailure(self._task_name, error=ret, grdst=grdst)


class ParallelTask(Task):
    def __init__(self, pool, *tasks, **kwargs):
        super(ParallelTask, self).__init__(**kwargs)
        self.subtasks = list(tasks)
        self.gevent_pool = pool

    def add(self, task):
        self.subtasks.append(task)

    def run(self):
        return self._run()

    def _run_one(self, task):
        ret = None
        try:
            ret = task.run()
        except Exception as e:
            ret = TaskFailure(task._task_name, error=e, grdst=None)
        if not ret.ok():
            self.ok = False
        return ret

    def _run(self):
        ret = self.gevent_pool.map(self._run_one, self.subtasks)
        if self.ok:
            return TaskSuccess(self._task_name, value=ret)
        else:
            grdst = self._try_guard()
            return TaskFailure(self._task_name, error=ret, grdst=grdst)


class RetryTask(Task):
    def __init__(self, *args, **kwargs):
        super(RetryTask, self).__init__(*args, **kwargs)
        self.retry_times = kwargs.get('retry_times', 1)

    def run(self):
        for i in range(self.retry_times):
            # Cleanup self.ok flag
            self.ok = True
            ret = super(RetryTask, self).run()
            if ret.ok():
                break
        return ret
