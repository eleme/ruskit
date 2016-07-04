import time

from ruskit import cli
from ..cluster import ClusterNode, Cluster
from ..utils import echo
from .create import InvalidNewNode


# nodes = [
#     {
#         "addr": "127.0.0.1:7000",
#         "role": "slave",
#         "master": "master_name"
#     }
# ]
def add_nodes(cluster, nodes):
    for node in nodes:
        cluster.add_node(node)

    echo("Adding node", end='')
    while not cluster.consistent():
        echo('.', end='')
        time.sleep(1)
    echo()
    cluster.wait()


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


@cli.command
@cli.argument("cluster")
@cli.argument("nodes", nargs='+')
@cli.pass_ctx
def add(ctx, args):
    nodes = format_nodes(args.nodes)
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))

    if not cluster.healthy():
        ctx.abort("Cluster not healthy.")

    try:
        add_nodes(cluster, nodes)
    except InvalidNewNode as e:
        echo("failed to add node: {}".format(e.message), color="red")
