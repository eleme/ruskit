from ..cluster import ClusterNode, Cluster
from ..utils import echo, Command


@Command.command
@Command.argument("cluster")
@Command.argument("nodes", nargs='+')
def delete(args):
    nodes = [ClusterNode.from_uri(n) for n in args.nodes]
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))

    if not cluster.healthy():
        echo("Cluster not healthy.")
        exit()

    echo("Deleting...")
    for node in nodes:
        cluster.delete_node(node)

    cluster.wait(verbose=True)
