import copy

from mock import patch

from ruskit.failover import FastAddMachineManager
from ruskit.distribute import RearrangeSlaveManager, NodeWrapper
from test_base import TestCaseBase


class DummyCluster(object):
    def __init__(self):
        self.nodes = []


def dummy_gen_distribution_for_move_masters(nodes, new_nodes):
    hosts = ['host1', 'host2', 'host3', 'host4']
    masters = [
        ['m1', 'm2', 'm3'],
        ['m4', 'm5', 'm6', 'm10', 'm11'],
        ['m7', 'm8', 'm9'],
        [],
    ]
    slaves = [
        ['s1'],
        ['s2'],
        ['s3'],
    ]
    frees = [[], [], [], ['f1', 'f2']]
    return {
        'hosts': hosts,
        'masters': masters,
        'slaves': slaves,
        'frees': frees,
    }

class MoveSlavesMockData(object):
    hosts = ['host1', 'host2', 'host3', 'host4']
    masters = [
        [NodeWrapper(i, 'm{}'.format(i), 0) for i in range(1, 4)],
        [NodeWrapper(i, 'm{}'.format(i), 1) for i in range(4, 8)],
        [NodeWrapper(i, 'm{}'.format(i), 2) for i in range(8, 11)],
        [NodeWrapper(i, 'm{}'.format(i), 3) for i in range(11, 14)],
    ]
    m1 = masters[0][0]
    m2 = masters[0][1]
    m5 = masters[1][1]
    m7 = masters[1][3]
    m8 = masters[2][0]
    m9 = masters[2][1]
    m11 = masters[3][0]
    s1, s2, s3, s4, s5, s6, s7 = (NodeWrapper(None, 's{}'.format(i), 1) \
                              for i in range(1, 8))
    s1.host_index = 0
    s6.host_index = 2
    s7.host_index = 2
    slaves = [
        [s1], [s2, s3, s4, s5], [s6, s7], []
    ]
    s1.master, s2.master, s3.master, s4.master = m7, m1, m9, m11
    s5.master, s6.master, s7.master = m2, m8, m5
    m1.slaves.append(s2)
    m2.slaves.append(s5)
    m5.slaves.append(s7)
    m7.slaves.append(s1)
    m8.slaves.append(s6)
    m9.slaves.append(s3)
    m11.slaves.append(s4)
    frees = [
        [NodeWrapper(None, 'f{}'.format(i), 0) for i in range(1, 5)],
        [NodeWrapper(None, 'f{}'.format(i), 1) for i in range(5, 8)],
        [NodeWrapper(None, 'f{}'.format(i), 2) for i in range(8, 12)],
        [NodeWrapper(None, 'f{}'.format(i), 3) for i in range(12, 16)],
    ]

    @classmethod
    def dummy_gen_distribution(cls, nodes, new_nodes):
        return {
            'hosts': map(copy.copy, cls.hosts),
            'masters': map(copy.copy, cls.masters),
            'slaves': map(copy.copy, cls.slaves),
            'frees': map(copy.copy, cls.frees),
        }


class TestMoveNode(TestCaseBase):
    @patch('ruskit.failover.gen_distribution',
        dummy_gen_distribution_for_move_masters)
    def test_move_master(self):
        dummy_new_nodes = []
        manager = FastAddMachineManager(DummyCluster(), dummy_new_nodes)
        result = manager.peek_result()
        plan = result['plan']
        p1 = plan[0]
        p2 = plan[1]
        if p1['master'] != 'm11':
            p1, p2 = p2, p1
        self.assertEqual(p1['master'], 'm11')
        self.assertEqual(p1['slave'], 'f2')
        self.assertEqual(p2['master'], 'm10')
        self.assertEqual(p2['slave'], 'f1')

    @patch('ruskit.distribute.gen_distribution',
        MoveSlavesMockData.dummy_gen_distribution)
    def test_move_slave(self):
        dummy_new_nodes = []
        manager = RearrangeSlaveManager(DummyCluster(), dummy_new_nodes)
        result = manager.gen_plan()
        add_plan = result['add_plan']
        delete_plan = result['delete_plan']
        # slaves_num == masters_num
        self.assertEqual(7 - len(delete_plan) + len(add_plan), 13)
        slaves_of_master = [1, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 0, 0]
        for slave in delete_plan:
            slaves_of_master[slave.master.node-1] -= 1
        for p in add_plan:
            slave = p['slave']
            master = p['master']
            slaves_of_master[master.node-1] += 1
        # All masters should have exactly one slave
        self.assertTrue(all(map(lambda s: s == 1, slaves_of_master)))
        dis = MoveSlavesMockData.dummy_gen_distribution(None, None)
        masters_per_host = [len(m) for m in dis['masters']]
        slaves_per_host = [len(set(s) - set(delete_plan)) \
            for s in dis['slaves']]
        new_slaves_per_host = [0 for _ in range(len(dis['hosts']))]
        for p in add_plan:
            new_slaves_per_host[p['slave'].host_index] += 1
        nodes_per_host = map(sum, zip(
            masters_per_host, slaves_per_host, new_slaves_per_host))
        # All hosts should contains almost the same number of nodes
        nodes_num = list(set(nodes_per_host))
        self.assertTrue(len(nodes_num) <= 2)
        if (len(nodes_num) == 2):
            self.assertTrue(abs(nodes_num[0] - nodes_num[1]) <= 1)
