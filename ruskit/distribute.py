import operator
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
        return '<NodeWrapper {}:{}>'.format(self.node.host, self.node.port)


class DistributionError(Exception):
    def __init__(self, missing):
        self.missing = missing

    def __str__(self):
        return 'not all masters have slaves: {}'.format(self.missing)


class MaxFlowSolver(object):
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

        self.vertex_count = 2 * host_count + 2
        self.s = self.vertex_count - 2
        self.t = self.vertex_count - 1
        self.result = []
        self.finished = False
        self.max_slaves_limit = max_slaves_limit

    def _gen_graph(self):
        # make it lazy to support optional addslaves command
        from igraph import Graph
        g = Graph().as_directed()
        g.add_vertices(self.vertex_count)
        g.es['weight'] = 1  # enable weight
        return g

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
            raise DistributionError(missing)

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

        self._sort_masters()
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

    def _sort_masters(self):
        for masters in self.masters:
            masters.sort(key=lambda m: len(m.slaves))


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
