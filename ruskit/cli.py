import argparse


class Context(object):
    def __init__(self, parser=None):
        self.__parser = parser

    def abort(self, message):
        if not self.__parser:
            return

        self.__parser.error(message)


class Command(object):
    def __init__(self, args, func):
        self.arguments = args
        self.parser = None
        self.name = func.__name__
        self.doc = func.__doc__
        self.func = func

    def callback(self, args):
        ctx = Context(self.parser)
        if hasattr(self.func, "__pass_ctx__"):
            self.func(ctx, args)
        else:
            self.func(args)

    @classmethod
    def command(cls, func):
        if not hasattr(func, "__cmd_args__"):
            func.__cmd_args__ = []
        func.__cmd_args__.reverse()
        return cls(func.__cmd_args__, func)

    @classmethod
    def pass_ctx(cls, func):
        func.__pass_ctx__ = True
        return func

    @classmethod
    def argument(cls, *args, **kwargs):
        def deco(func):
            if not hasattr(func, "__cmd_args__"):
                func.__cmd_args__ = []
            func.__cmd_args__.append((args, kwargs))
            return func
        return deco

    def __call__(self):
        self.parser = argparse.ArgumentParser()
        for args, kwargs in self.arguments:
            self.parser.add_argument(*args, **kwargs)
        args = self.parser.parse_args()
        self.callback(args)


class CommandParser(object):
    def __init__(self, *args, **kwargs):
        self.parser = argparse.ArgumentParser(*args, **kwargs)
        self.subparser = self.parser.add_subparsers(title="Subcommands")

    def add_command(self, command):
        parser = self.subparser.add_parser(command.name, help=command.doc)
        command.parser = parser
        for args, kwargs in command.arguments:
            parser.add_argument(*args, **kwargs)
        parser.set_defaults(func=command.callback)

    def run(self):
        args = self.parser.parse_args()
        args.func(args)


# convinent alias
command = Command.command
argument = Command.argument
pass_ctx = Command.pass_ctx
