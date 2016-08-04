from ruskit import cli
from ..cluster import Cluster, ClusterNode
from ..distribute import MaxFlowSolver, print_cluster, gen_distribution
from ..utils import echo, timeout_argument


class AddMastersManager(object):
    def __init__(self, cluster, new_nodes):
        self.cluster = cluster
        self.new_nodes = new_nodes

    def add_masters(self):
        nodes = []
        for n in self.new_nodes:
            nodes.append({
                'cluster_node': n,
                'role': 'master',
            })
        self.cluster.add_nodes(nodes)
        self.cluster.wait()

    def get_distribution(self):
        return gen_distribution(self.cluster.nodes, self.new_nodes)


class AddSlavesManager(object):
    def __init__(self, cluster, new_nodes, max_slaves_limit):
        self.cluster = cluster
        self.new_nodes = new_nodes
        self.solver = MaxFlowSolver.from_nodes(
            cluster.nodes, new_nodes, max_slaves_limit)

    def peek_result(self):
        result, frees = self.solver.distribute_slaves()
        return result, frees

    def add_slaves(self):
        result, frees = self.solver.distribute_slaves()
        nodes = []
        for free, master in result:
            nodes.append({
                'cluster_node': free.node,
                'role': 'slave',
                'master': master.name,
            })
        self.cluster.add_nodes(nodes)
        self.cluster.wait()

    def get_distribution(self):
        return self.solver.get_distribution()

def gen_nodes_from_args(nodes):
    new_nodes = []
    for n in nodes:
        host, port = n.split(':')
        new_nodes.append(ClusterNode(host, port))
    return new_nodes


@cli.command
@cli.argument("-p", "--peek", dest="peek", default=False, action="store_true")
@cli.argument("-l", "--slaves-limit", dest="slaves_limit",
    default=None, type=int)
@cli.argument("cluster")
@cli.argument("nodes", nargs='+')
@timeout_argument
@cli.pass_ctx
def addslave(ctx, args):
    new_nodes = gen_nodes_from_args(args.nodes)
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))

    if not cluster.healthy():
        ctx.abort("Cluster not healthy.")

    manager = AddSlavesManager(cluster, new_nodes, args.slaves_limit)
    if args.peek:
        echo('before', color='purple')
        print_cluster(manager.get_distribution())
        result, frees = manager.peek_result()
        echo('after', color='purple')
        print_cluster(manager.get_distribution())
    else:
        manager.add_slaves()
