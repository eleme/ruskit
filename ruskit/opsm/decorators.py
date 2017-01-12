# -*- coding: utf-8 -*-

import functools

from . import exceptions


def enable_failure_unwrap(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            ret = f(*args, **kwargs)
        except exceptions.OPSMReturnOnErrorShortcutException as e:
            return e.failure
        return ret
    return wrapper
