import redis

from ..cluster import Cluster, ClusterNode
from ..utils import Command, echo


@Command.command
@Command.argument("src")
@Command.argument("-d", "--dst")
@Command.argument("-s", "--slot", type=int)
def migrate(args):
    src = ClusterNode.from_uri(args.src)
    cluster = Cluster.from_node(src)

    if args.dst:
        dst = ClusterNode.from_uri(args.dst)

    if args.dst and args.slot is not None:
        try:
            cluster.migrate_slot(src, dst, args.slot, verbose=True)
        except redis.ResponseError as e:
            echo(str(e))
    elif args.dst:
        cluster.migrate(src, dst, len(src.slots))
    else:
        cluster.migrate_node(src)

    cluster.wait(verbose=True)


if __name__ == "__main__":
    migrate()
