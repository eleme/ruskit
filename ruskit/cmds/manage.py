from ..cluster import Cluster, ClusterNode
from ..utils import Command, echo


@Command.command
@Command.argument("cluster")
def info(args):
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    dis = ["  {}:{}: {}".format(n.host, n.port, len(n.slots))
           for n in cluster.masters]
    echo("Slots distribution:")
    echo("\n".join(dis))
    echo("Total slots:", sum(len(n.slots) for n in cluster.masters))


@Command.command
@Command.argument("cluster")
def fix(args):
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    cluster.reshard()
    cluster.fill_slots()
