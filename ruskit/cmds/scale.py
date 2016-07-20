from ruskit import cli
from ..cluster import Cluster, ClusterNode

from ..distribute import MaxFlowSolver, print_cluster


class ScaleManager(object):
    def __init__(self, cluster, new_nodes):
        self.cluster = cluster
        self.new_nodes = new_nodes
        self.solver = MaxFlowSolver.from_nodes(cluster.nodes, new_nodes)

    def check_new_nodes(self):
        pass

    def peek_result(self):
        result, frees = self.solver.distribute_slaves()
        return result, frees


def gen_nodes_from_args(nodes):
    new_nodes = []
    for n in nodes:
        host, port = n.split(':')
        new_nodes.append(ClusterNode(host, port))
    return new_nodes


@cli.command
@cli.argument("cluster")
@cli.argument("master_count", type=int)
@cli.argument("nodes", nargs='+')
@cli.pass_ctx
def scale(ctx, args):
    new_nodes = gen_nodes_from_args(args.nodes)
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))

    if not cluster.healthy():
        ctx.abort("Cluster not healthy.")

    manager = ScaleManager(cluster, new_nodes)
    print_cluster(*manager.solver.get_distribution())
    result, frees = manager.peek_result()
    print_cluster(*manager.solver.get_distribution())
