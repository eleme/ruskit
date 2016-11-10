import operator
from copy import copy
from itertools import imap, repeat, izip_longest

from .cluster import Cluster, ClusterNode
from .utils import echo


class NodeWrapper(object):
    def __init__(self, node, tag, host_index, master=None):
        self.node = node
        self.tag = tag
        self.host_index = host_index
        self.master = master
        self.slaves = []

    def __getattr__(self, attr):
        return getattr(self.node, attr)

    def __repr__(self):
        if not (hasattr(self.node, 'host') and hasattr(self.node, 'port')):
            return self.debug_repr()  # for mock node
        if self.master:
            return '<NodeWrapper {} -> {}>'.format(self.node.gen_addr(),
                                                   self.master.gen_addr())
        else:
            return '<NodeWrapper {}>'.format(self.node.gen_addr())

    def debug_repr(self):
        if self.master:
            return '<NodeWrapper {} -> {}>'.format(self.tag, self.master.tag)
        else:
            return '<NodeWrapper {}>'.format(self.tag)


class DistributionError(Exception):
    pass


class MissingSlaves(DistributionError):
    def __init__(self, missing):
        self.missing = missing

    def __str__(self):
        return 'not all masters have slaves: {}'.format(self.missing)


class InvalidMasterDistribution(DistributionError):
    pass


class MaxFlowBase(object):
    def __init__(self, host_count):
        self.vertex_count = 2 * host_count + 2
        self.s = self.vertex_count - 2
        self.t = self.vertex_count - 1

    def _gen_graph(self):
        # make it lazy to support optional addslaves command
        from igraph import Graph
        g = Graph().as_directed()
        g.add_vertices(self.vertex_count)
        g.es['weight'] = 1  # enable weight
        return g

    @staticmethod
    def _sort_masters(masters):
        for ms in masters:
            ms.sort(key=lambda m: len(m.slaves))


class MaxFlowSolver(MaxFlowBase):
    '''Slaves distribution problem can be converted to max flow problem'''

    @classmethod
    def from_nodes(cls, nodes, new_nodes, max_slaves_limit=None):
        '''When this is used only for peeking reuslt
           `new_nodes` can be any type with `host` and `port` attributes
        '''
        param = gen_distribution(nodes, new_nodes)
        param['max_slaves_limit'] = max_slaves_limit
        return cls(**param)

    def __init__(self, hosts, masters, slaves, frees, max_slaves_limit):
        host_count = len(hosts)
        self.hosts = hosts
        self.host_count = host_count
        self.masters = masters
        self.slaves = slaves
        self.frees = frees
        self.orphans = [
            [m for m in machine if len(m.slaves) == 0] for machine in masters]
        self.orphans_count = len(sum(self.orphans, []))
        self.slice_tags = []
        for ss in slaves:
            self.slice_tags.append([s.master.tag for s in ss])

        super(MaxFlowSolver, self).__init__(host_count)
        self.result = []
        self.finished = False
        self.max_slaves_limit = max_slaves_limit

    def get_distribution(self):
        return {
            'hosts': self.hosts,
            'masters': self.masters,
            'slaves': self.slaves,
            'frees': self.frees,
        }

    def distribute_slaves(self):
        if self.finished:
            return self.result, self.remaining_frees
        if self.orphans_count > 0:
            self._fill_orphans()
        self._fill_remaining()
        self.finished = True
        return self.result, self.remaining_frees

    def _fill_orphans(self):
        g = self._gen_graph()
        ct = self.host_count
        flow_in = map(len, self.frees)
        flow_out = map(len, self.orphans)  # set it to orphans first
        # build graph
        for i, c in enumerate(flow_in):
            g[self.s, i] = c
        for i, c in enumerate(flow_out):
            g[i + ct, self.t] = c
        for i in xrange(ct):
            for j in xrange(ct):
                if i == j:
                    continue
                g[i, ct + j] = len(self.frees[i])

        mf = g.maxflow(self.s, self.t, g.es['weight'])
        if mf.value < self.orphans_count:
            missing = self._gen_hosts_missing_slaves(g, mf)
            raise MissingSlaves(missing)

        for edge_index, e in enumerate(g.es):
            if e.source == self.s or e.target == self.t:
                continue
            for _ in xrange(int(mf.flow[edge_index])):
                f = self.frees[e.source].pop(0)
                o = self.orphans[e.target - ct].pop()
                f.master = o
                o.slaves.append(f)
                self.slaves[e.source].append(f)
                self.slice_tags[e.source].append(o.tag)
                self.result.append((f, o))
        # check all masters have slaves
        assert len(sum(self.orphans, [])) == 0
        assert all(len(m.slaves) > 0 for m in sum(self.masters, []))

    def _gen_hosts_missing_slaves(self, g, maxflow):
        ct = self.host_count
        flows = list(repeat(0, ct))
        for edge_index, e in enumerate(g.es):
            if e.source == self.s or e.target == self.t:
                continue
            flows[e.target - ct] += int(maxflow.flow[edge_index])
        missing = map(operator.sub, map(len, self.orphans), flows)
        missing = zip(self.hosts, missing)
        return [m for m in missing if m[1] > 0]

    def _fill_remaining(self):
        g = self._gen_graph()
        ct = self.host_count
        flow_in = map(len, self.frees)
        if self.max_slaves_limit is None:
            flow_out = list(repeat(len(sum(self.frees, [])), ct))
        else:
            flow_out = self._gen_limits()
        for i, c in enumerate(flow_in):
            g[self.s, i] = c
        for i, c in enumerate(flow_out):
            g[i + ct, self.t] = c
        for i in xrange(ct):
            for j in xrange(ct):
                if i == j:
                    continue
                masters_in_j = set(
                    s.master.tag for s in self.slaves[i] \
                    if s.master.host_index == j)
                limit = len(self.masters[j]) - len(masters_in_j)
                g[i, ct + j] = min(len(self.frees[i]), limit)
        mf = g.maxflow(self.s, self.t, g.es['weight'])

        self._sort_masters(self.masters)
        for edge_index, e in enumerate(g.es):
            if e.source == self.s or e.target == self.t:
                continue
            for _ in xrange(int(mf.flow[edge_index])):
                f = self.frees[e.source].pop(0)
                m = next((m for m in self.masters[e.target - ct] \
                    if m.tag not in self.slice_tags[e.source]), None)
                assert m is not None
                f.master = m
                m.slaves.append(f)
                self.slaves[e.source].append(f)
                self.result.append((f, m))
                self.slice_tags[e.source].append(m.tag)
                self.masters[e.target - ct].sort(key=lambda m: len(m.slaves))

        self.remaining_frees = sum(self.frees, [])

    def _gen_limits(self):
        limits = []
        for masters in self.masters:
            limits.append(sum(
                max(self.max_slaves_limit - len(m.slaves), 0) \
                for m in masters))
        return limits


def gen_distribution(nodes, new_nodes):
    hosts = list({n.host for n in nodes + new_nodes})
    host_count = len(hosts)
    host_indices = {h: i for i, h in enumerate(hosts)}
    masters = [[] for _ in xrange(host_count)]
    slaves = [[] for _ in xrange(host_count)]
    frees = [[] for _ in xrange(host_count)]
    masters_map = {}
    for n in nodes:
        if not n.is_master():
            continue
        host_index = host_indices[n.host]
        master = NodeWrapper(n, n.node_info['name'], host_index)
        masters[host_index].append(master)
        masters_map[n.node_info['name']] = master
    for n in nodes:
        if n.is_master():
            continue
        host_index = host_indices[n.host]
        master = masters_map[n.node_info['replicate']]
        slave = NodeWrapper(n, n.node_info['name'], host_index, master)
        master.slaves.append(slave)
        slaves[host_index].append(slave)
    for i, n in enumerate(new_nodes):
        host_index = host_indices[n.host]
        # use dummy name for new nodes to avoid getting info from them
        frees[host_index].append(
            NodeWrapper(n, i, host_index))
    return {
        'hosts': hosts,
        'masters': masters,
        'slaves': slaves,
        'frees': frees,
    }


def print_cluster(distribution):
    hosts = distribution['hosts']
    masters = distribution['masters']
    slaves = distribution['slaves']
    frees = distribution['frees']
    ms = [['m{}'.format(n.port) for n in h] for h in masters]
    ss = [['s{}->({}):{}'.format(n.port, n.master.host_index, n.master.port) \
        for n in h] for h in slaves]
    fs = [['f{}'.format(f.port) for f in h] for h in frees]
    all_node = map(operator.add, ms, ss)
    all_node = map(operator.add, all_node, fs)
    fmt = "{:<20}"
    print (fmt * len(hosts)).format(
        *['{}({})'.format(h, i) for i, h in enumerate(hosts)])
    color_map = {
        'm': 'red',
        's': 'yellow',
        'f': 'green',
    }
    for line in izip_longest(*all_node, fillvalue=' '):
        for c in line:
            echo(fmt.format(c), color=color_map.get(c[0]) or None, end='')
        print ''


class RearrangeSlaveManager(MaxFlowBase):
    '''Change all masters to have exactly one slave'''
    def __init__(self, cluster, new_nodes):
        self.cluster = cluster
        self.new_nodes = new_nodes
        self.result = None
        self.dis = gen_distribution(self.cluster.nodes, self.new_nodes)
        super(RearrangeSlaveManager, self).__init__(len(self.dis['hosts']))

    def check_masters_distribution():
        masters_per_host = [len(m) for m in self.dis['masters']]
        masters_num = list(set(masters_per_host))
        if len(nodes_num) > 2:
            raise InvalidMasterDistribution()
        if len(nodes_num == 2) and abs(nodes_num[0] - nodes_num[1]) > 1:
            raise InvalidMasterDistribution()

    def gen_distribution(self):
        return gen_distribution(self.cluster.nodes, self.new_nodes)

    def gen_plan(self):
        if self.result:
            return self.result

        add_plan = []
        delete_plan = []
        hosts = map(copy, self.dis['hosts'])
        masters = map(copy, self.dis['masters'])
        slaves = map(copy, self.dis['slaves'])
        frees = map(copy, self.dis['frees'])
        ct = len(hosts)
        masters_per_host = (len(sum(masters, [])) + ct - 1) / ct
        limit = 1 + masters_per_host / ct

        # first assume that there is no slave for every master
        g = self._gen_graph()
        flow_in = map(len, frees)
        flow_out = map(len, masters)
        for i, c in enumerate(flow_in):
            g[self.s, i] = c
        for i, c in enumerate(flow_out):
            g[i + ct, self.t] = c
        for i in xrange(ct):
            for j in xrange(ct):
                if i == j:
                    continue
                g[i, ct + j] = min(len(frees[i]), limit, len(masters[j]))
        mf = g.maxflow(self.s, self.t, g.es['weight'])

        curr_map = self._gen_curr_map()
        edges = []
        for edge_index, e in enumerate(g.es):
            if e.source == self.s or e.target == self.t:
                continue
            i, j = e.source, e.target - ct
            flow = int(mf.flow[edge_index])
            edges.append((i, j, flow))

        # delete redundant slaves
        for i, j, flow in edges:
            if curr_map[i][j] > flow:
                deleted_num = curr_map[i][j] - flow
                deleted_slaves = [s for s in slaves[i] \
                    if s.master.host_index == j][:deleted_num]
                for s in deleted_slaves:
                    s.master.slaves.remove(s)
                delete_plan.extend(deleted_slaves)

        # add new slaves
        self._sort_masters(masters)
        for i, j, flow in edges:
            for _ in xrange(max(0, flow - curr_map[i][j])):
                m = masters[j].pop(0)
                if len(m.slaves) > 0:
                    break
                f = frees[i].pop(0)
                m.slaves.append(f)
                f.master = m
                add_plan.append({'slave': f, 'master': m})

        self.assert_plan(delete_plan, add_plan)
        return {
            'add_plan': add_plan,
            'delete_plan': delete_plan,
            'frees': frees
        }

    def _gen_curr_map(self):
        hosts_num = len(self.dis['hosts'])
        curr_graph = [list(repeat(0, hosts_num)) for _ in xrange(hosts_num)]
        for i, slaves in enumerate(self.dis['slaves']):
            for s in slaves:
                curr_graph[i][s.master.host_index] += 1
        return curr_graph

    def assert_plan(self, delete_plan, add_plan):
        dis = gen_distribution(self.cluster.nodes, self.new_nodes)
        slaves = sum(dis['slaves'], [])
        masters = sum(dis['masters'], [])

        slaves_num = len(slaves) - len(delete_plan) + len(add_plan)
        if slaves_num != len(masters):
            raise DistributionError()
