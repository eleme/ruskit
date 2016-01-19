import redis
import pprint

from ruskit import cli
from ..cluster import Cluster, ClusterNode
from ..utils import echo


@cli.command
@cli.argument("cluster")
def info(args):
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    dis = []
    for n in cluster.masters:
        slaves = ','.join([s["addr"] for s in n.slaves(n.name)])
        msg = "{} {}:{} {} {}".format(n.name, n.host, n.port, len(n.slots),
                                      slaves)
        dis.append(msg)
    echo("\n".join(dis))
    echo("Masters:", len(cluster.masters))
    echo("Instances:", len(cluster.nodes))
    echo("Slots:", sum(len(n.slots) for n in cluster.masters))

    random_node = cluster.nodes[0]
    connection_pool = random_node.r.connection_pool
    connection = connection_pool._available_connections[0]
    echo('Exception Classes:')
    pprint.pprint(connection._parser.EXCEPTION_CLASSES)


@cli.command
@cli.argument("cluster")
def fix(args):
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    cluster.fix_open_slots()
    cluster.fill_slots()


@cli.command
@cli.argument("cluster")
@cli.argument("nodes", nargs='+')
def delete(args):
    """Delete nodes from the cluster
    """
    nodes = [ClusterNode.from_uri(n) for n in args.nodes]
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))

    echo("Deleting...")
    for node in nodes:
        cluster.delete_node(node)
        cluster.wait()


@cli.command
@cli.argument("src")
@cli.argument("-d", "--dst")
@cli.argument("-s", "--slot", type=int)
@cli.argument("-c", "--count", type=int)
@cli.argument("-i", "--income", action="store_true")
@cli.pass_ctx
def migrate(ctx, args):
    src = ClusterNode.from_uri(args.src)
    cluster = Cluster.from_node(src)

    if args.dst:
        dst = ClusterNode.from_uri(args.dst)

    if args.dst and args.slot is not None:
        try:
            cluster.migrate_slot(src, dst, args.slot, verbose=True)
        except redis.ResponseError as e:
            ctx.abort(str(e))
    elif args.dst:
        count = len(src.slots) if args.count is None else args.count
        cluster.migrate(src, dst, count)
    else:
        cluster.migrate_node(src, args.count, income=args.income)

    cluster.wait()


@cli.command
@cli.argument("cluster")
def reshard(args):
    """Balance slots in the cluster.

    This command will try its best to distribute slots equally.
    """
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    cluster.reshard()


@cli.command
@cli.argument("node")
@cli.argument("master")
@cli.pass_ctx
def replicate(ctx, args):
    """Make node to be the slave of a master.
    """
    slave = ClusterNode.from_uri(args.node)
    master = ClusterNode.from_uri(args.master)
    if not master.is_master():
        ctx.abort("Node {!r} is not a master.".format(args.master))

    try:
        slave.replicate(master.name)
    except redis.ResponseError as e:
        ctx.abort(str(e))

    Cluster.from_node(master).wait()


@cli.command
@cli.argument("cluster")
def destroy(args):
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    for node in cluster.masters:
        node.flushall()

    for node in cluster.nodes:
        node.reset(hard=True)
