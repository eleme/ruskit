from __future__ import print_function

import argparse
import itertools
import os
import sys

COLOR_MAP = {
    "red": 31,
    "green": 32,
    "yellow": 33,
    "blue": 34,
    "purple": 35
}


def echo(*values, **kwargs):
    end = kwargs.get("end", '\n')
    color = kwargs.get("color", None)
    bold = 0 if kwargs.get("bold", False) is False else 1
    disable = kwargs.get("diable", False)

    if disable:
        return

    msg = ' '.join(str(v) for v in values) + end

    if not color or os.getenv("ANSI_COLORS_DISABLED") is not None:
        sys.stdout.write(msg)
    else:
        color_prefix = "\033[{};{}m".format(bold, COLOR_MAP[color])
        color_suffix = "\033[0m"
        sys.stdout.write(color_prefix + msg + color_suffix)
    sys.stdout.flush()


def divide_number(n, m):
    avg = int(n / m)
    remain = n - m * avg
    data = list(itertools.repeat(avg, m))
    for i in range(len(data)):
        if not remain:
            break
        data[i] += 1
        remain -= 1
    return data


class Command(object):
    def __init__(self, args, func):
        self.arguments = args
        self.name = func.__name__
        self.callback = func

    @classmethod
    def command(cls, func):
        if not hasattr(func, "__cmd_args__"):
            func.__cmd_args__ = []
        func.__cmd_args__.reverse()
        return cls(func.__cmd_args__, func)

    @classmethod
    def argument(cls, *args, **kwargs):
        def deco(func):
            if not hasattr(func, "__cmd_args__"):
                func.__cmd_args__ = []
            func.__cmd_args__.append((args, kwargs))
            return func
        return deco

    def __call__(self):
        parser = argparse.ArgumentParser()
        for args, kwargs in self.arguments:
            parser.add_argument(*args, **kwargs)
        args = parser.parse_args()
        self.callback(args)


class CommandParser(object):
    def __init__(self, *args, **kwargs):
        self.parser = argparse.ArgumentParser(*args, **kwargs)
        self.subparser = self.parser.add_subparsers()

    def add_command(self, command):
        parser = self.subparser.add_parser(command.name)
        for args, kwargs in command.arguments:
            parser.add_argument(*args, **kwargs)
        parser.set_defaults(func=command.callback)

    def run(self):
        args = self.parser.parse_args()
        args.func(args)
