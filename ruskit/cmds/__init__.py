from ..utils import CommandParser

from .add import add
from .delete import delete
from .create import create
from .migrate import migrate
from .manage import info, fix


def main():
    parser = CommandParser(fromfile_prefix_chars='@')
    parser.add_command(add)
    parser.add_command(delete)
    parser.add_command(create)
    parser.add_command(migrate)
    parser.add_command(info)
    parser.add_command(fix)

    parser.run()


if __name__ == "__main__":
    main()
