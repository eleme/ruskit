from ..cli import CommandParser

from .add import add
from .create import create
from .scale import addslave
from .manage import (
    info, fix, migrate, delete, reshard, replicate, destroy, flushall, slowlog,
    reconfigure, peek
    )


def main():
    parser = CommandParser(fromfile_prefix_chars='@')
    parser.add_command(add)
    parser.add_command(delete)
    parser.add_command(create)
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

    parser.run()


if __name__ == "__main__":
    main()
