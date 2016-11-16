from itertools import repeat

from ruskit import cli
from ..cluster import Cluster, ClusterNode
from ..distribute import (MaxFlowSolver, print_cluster, gen_distribution,
    RearrangeSlaveManager)
from ..failover import FastAddMachineManager
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
        match, frees = self.solver.distribute_slaves()
        match = [{
            'master': m,
            'slave': s,
        } for s, m in match]
        return {
            'match': match,
            'frees': frees,
        }

    def add_slaves(self, fast_mode=False):
        result, frees = self.solver.distribute_slaves()
        nodes = []
        for free, master in result:
            nodes.append({
                'cluster_node': free.node,
                'role': 'slave',
                'master': master.name,
            })
        self.cluster.add_slaves(nodes, fast_mode)
        self.cluster.wait()

    def get_distribution(self):
        return self.solver.get_distribution()

def gen_nodes_from_args(nodes):
    new_nodes = []
    for n in nodes:
        host, port = n.split(':')
        new_nodes.append(ClusterNode(host, port))
    return new_nodes


class MoveSlaveManager(object):
    def __init__(self, cluster, new_nodes, fast_mode=False):
        self.cluster = cluster
        self.new_nodes = new_nodes
        self.fast_mode = fast_mode
        self.manager = RearrangeSlaveManager(cluster, new_nodes)

    def peek_result(self):
        plan = self.manager.gen_plan()
        delete_plan = plan['delete_plan']
        add_plan = plan['add_plan']
        dis = self.manager.dis
        masters = dis['masters']
        old_slaves = dis['slaves']
        old_slaves = [list(set(s) - set(delete_plan)) for s in old_slaves]
        new_slaves = [[] for _ in xrange(len(masters))]
        for plan in add_plan:
            slave = plan['slave']
            new_slaves[slave.host_index].append(slave)
        return {
            'hosts': dis['hosts'],
            'masters': masters,
            'slaves': [o + n for o, n in zip(old_slaves, new_slaves)],
            'frees': list(repeat([], len(masters))),
        }

    def move_slaves(self):
        plan = self.manager.gen_plan()
        if self.fast_mode:
            self.delete_slaves(plan['delete_plan'])
            self.add_slaves(plan['add_plan'])
        else:
            self.add_slaves(plan['add_plan'])
            self.delete_slaves(plan['delete_plan'])

    def delete_slaves(self, delete_plan):
        for slave in delete_plan:
            self.cluster.delete_node(slave)

    def add_slaves(self, add_plan):
        nodes = []
        for plan in add_plan:
            free = plan['slave']
            master = plan['master']
            nodes.append({
                'cluster_node': free.node,
                'role': 'slave',
                'master': master.name,
            })
        self.cluster.add_slaves(nodes, self.fast_mode)
        self.cluster.wait()


@cli.command
@cli.argument("-p", "--peek", dest="peek", default=False, action="store_true")
@cli.argument("-l", "--slaves-limit", dest="slaves_limit",
    default=None, type=int)
@cli.argument("-f", "--fast-mode", dest="fast_mode", default=False,
    action="store_true")
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
        result = manager.peek_result()
        echo('after', color='purple')
        print_cluster(manager.get_distribution())
    else:
        manager.add_slaves(fast_mode=args.fast_mode)


@cli.command
@cli.argument("-p", "--peek", dest="peek", default=False, action="store_true")
@cli.argument("-f", "--fast-mode", dest="fast_mode", default=False,
    action="store_true")
@cli.argument("cluster")
@cli.argument("nodes", nargs='+')
@timeout_argument
def movemaster(args):
    new_nodes = gen_nodes_from_args(args.nodes)
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))

    if not cluster.healthy():
        logger.warn('Cluster not healthy.')

    manager = FastAddMachineManager(cluster, new_nodes)
    if args.peek:
        print_cluster(manager.get_distribution())
        result = manager.peek_result()
        print 'plan: ', result['plan']
    else:
        manager.move_masters_to_new_hosts(fast_mode=args.fast_mode)


@cli.command
@cli.argument("-p", "--peek", dest="peek", default=False, action="store_true")
@cli.argument("-f", "--fast-mode", dest="fast_mode", default=False,
    action="store_true")
@cli.argument("cluster")
@cli.argument("nodes", nargs='+')
@timeout_argument
def moveslave(args):
    new_nodes = gen_nodes_from_args(args.nodes)
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))

    if not cluster.healthy():
        logger.warn('Cluster not healthy.')

    manager = MoveSlaveManager(cluster, new_nodes, args.fast_mode)
    if args.peek:
        print_cluster(manager.peek_result())
    else:
        manager.move_slaves()
