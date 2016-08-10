import logging

from ..cli import CommandParser
from .add import add as add_cmd  # name conflict with module
from .create import create as create_cmd  # name conflict with module
from .scale import addslave
from .manage import (
    info, fix, migrate, delete, reshard, replicate, destroy, flushall, slowlog,
    reconfigure, peek
    )


def gen_parser():
    parser = CommandParser(fromfile_prefix_chars='@')
    parser.add_command(add_cmd)
    parser.add_command(delete)
    parser.add_command(create_cmd)
    parser.add_command(migrate)
    parser.add_command(info)
    parser.add_command(fix)
    parser.add_command(reshard)
    parser.add_command(replicate)
    parser.add_command(destroy)
    parser.add_command(slowlog)
    parser.add_command(reconfigure)
    parser.add_command(flushall)
    parser.add_command(peek)
    parser.add_command(addslave)
    return parser


def main():
    logging.basicConfig()
    parser = gen_parser()
    parser.run()


if __name__ == "__main__":
    main()
