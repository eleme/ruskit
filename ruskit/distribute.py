import operator
from itertools import imap, repeat, izip_longest

from igraph import Graph

from .cluster import Cluster, ClusterNode


class NodeWrapper(object):
    def __init__(self, node, tag, host_index, master=None):
        self.node = node
        self.tag = tag
        self.host_index = host_index
        self.master = master
        self.slaves = []

    def __getattr__(self, attr):
        return getattr(self.node, attr)


class DistributionError(Exception):
    pass


class MaxFlowSolver(object):
    '''Slaves distribution problem can be converted to max flow problem'''

    @classmethod
    def from_nodes(cls, nodes, new_nodes):
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
        for n in new_nodes:
            host_index = host_indices[n.host]
            frees[host_index].append(
                NodeWrapper(n, n.node_info['name'], host_index))
        return cls(hosts, masters, slaves, frees)

    def __init__(self, hosts, masters, slaves, frees):
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

        self.vertex_count = 2 * host_count + 2
        self.s = self.vertex_count - 2
        self.t = self.vertex_count - 1
        self.result = []
        self.finished = False

    def _gen_graph(self):
        g = Graph().as_directed()
        g.add_vertices(self.vertex_count)
        g.es['weight'] = 1  # enable weight
        return g

    def get_distribution(self):
        return self.hosts, self.masters, self.slaves, self.frees

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
            raise DistributionError('unable to distribute slaves')

        for edge_index, e in enumerate(g.es):
            if e.source == self.s or e.target == self.t:
                continue
            for _ in xrange(int(mf.flow[edge_index])):
                f = self.frees[e.source].pop()
                o = self.orphans[e.target - ct].pop()
                f.master = o
                o.slaves.append(f)
                self.slaves[e.source].append(f)
                self.slice_tags[e.source].append(o.tag)
                self.result.append((f, o))
        # check all masters have slaves
        assert len(sum(self.orphans, [])) == 0
        assert all(len(m.slaves) > 0 for m in sum(self.masters, []))

    def _fill_remaining(self):
        g = self._gen_graph()
        ct = self.host_count
        flow_in = map(len, self.frees)
        flow_out = list(repeat(len(sum(self.frees, [])), ct))
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

        self._sort_masters()
        for edge_index, e in enumerate(g.es):
            if e.source == self.s or e.target == self.t:
                continue
            for _ in xrange(int(mf.flow[edge_index])):
                f = self.frees[e.source].pop()
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

    def _sort_masters(self):
        for masters in self.masters:
            masters.sort(key=lambda m: len(m.slaves))


def print_cluster(hosts, masters, slaves, frees):
    ms = [['m{}'.format(n.port) for n in h] for h in masters]
    ss = [['s{}->({}):{}'.format(n.port, n.master.host_index, n.master.port) \
        for n in h] for h in slaves]
    fs = [['f{}'.format(f.port) for f in h] for h in frees]
    all_node = map(operator.add, ms, ss)
    all_node = map(operator.add, all_node, fs)
    fmt = "{:<20}" * len(hosts)
    print fmt.format(*['{}({})'.format(h, i) for i, h in enumerate(hosts)])
    for line in izip_longest(*all_node, fillvalue=' '):
        print fmt.format(*line)
