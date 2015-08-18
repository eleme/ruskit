import time

from ..cluster import ClusterNode, Cluster
from ..utils import echo, Command


# nodes = [
#     {
#         "addr": "127.0.0.1:7000",
#         "role": "slave",
#         "master": "master_name"
#     }
# ]
def add_nodes(cluster, nodes):
    if not cluster.healthy():
        echo("Cluster not healthy.")
        exit()

    for node in nodes:
        cluster.add_node(node)

    echo("Adding node", end='')
    while not cluster.consistent():
        echo('.', end='')
        time.sleep(1)
    echo()

    echo("Resharding...")
    cluster.reshard()

    cluster.wait(verbose=True)


def format_nodes(nodes):
    data = []
    for node in nodes:
        res = {}
        if ',' in node:
            n, m = node.split(',')
            res["addr"] = n
            res["role"] = "slave"
            res["master"] = ClusterNode.from_uri(m).name
        else:
            res["addr"] = node
            res["role"] = "master"
        data.append(res)
    return data


@Command.command
@Command.argument("cluster")
@Command.argument("nodes", nargs='+')
def add(args):
    nodes = format_nodes(args.nodes)
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    add_nodes(cluster, nodes)
    dis = ["{}:{}: {}".format(n.host, n.port, len(n.slots))
           for n in cluster.masters]
    echo("Slots distribution:")
    echo("\n".join(dis))
