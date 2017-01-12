# -*- coding: utf-8 -*-


def is_iterable_not_str(obj):
    return hasattr(obj, '__iter__') and not isinstance(obj, str)
