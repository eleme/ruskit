from mock import patch

from ruskit.failover import FastAddMachineManager
from test_base import TestCaseBase


class DummyCluster(object):
    def __init__(self):
        self.nodes = []


def dummy_gen_distribution(nodes, new_nodes):
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


class TestMoveMaster(TestCaseBase):
    @patch('ruskit.failover.gen_distribution', dummy_gen_distribution)
    def test_gen_plan(self):
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
