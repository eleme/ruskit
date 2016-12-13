# -*- coding: utf-8 -*-
import sys
if sys.version_info.major < 3:
    # Circular import issue in python 2, but works in python 3
    # Do not use absolute_import: from __future__ import absolute_import
    import lib
else:
    from . import lib


class OPSMReturnOnErrorShortcutException(Exception):
    def __init__(self, failure):
        assert isinstance(failure, lib.TaskFailure)
        self.failure = failure

    def __str__(self):
        return 'OPSM_ROESE: {}'.format(self.failure)


class PreviousTaskFailedError(Exception):
    def __init__(self, msg='PTF'):
        self.msg = msg

    def __str__(self):
        return self.msg
