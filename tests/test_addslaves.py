from mock import patch

from ruskit.cluster import ClusterNode, Cluster
from ruskit.distribute import MaxFlowSolver, DistributionError
from test_base import TestCaseBase, MockNewNode


class TestAddSlaves(TestCaseBase):
    @patch.object(Cluster, 'wait')
    @patch.object(Cluster, '_wait_nodes_updated')
    def test_add_nodes(self, m1, m2):
        new = self.new_nodes[0]
        self.cluster.add_nodes([{
            'cluster_node': new,
            'role': 'slave',
            'master': self.cluster.nodes[1].name,
        }])
        self.assert_exec_cmd(new, 'CLUSTER MEET', 'host0', 6000)
        self.assert_exec_cmd(new,
            'CLUSTER REPLICATE', self.cluster.nodes[1].name)

    @patch.object(Cluster, 'wait')
    @patch.object(Cluster, '_wait_nodes_updated')
    def test_add_nodes_in_slow_mode(self, m1, m2):
        new = self.new_nodes[0]
        self.cluster.add_slaves([{
            'cluster_node': new,
            'role': 'slave',
            'master': self.cluster.nodes[1].name,
        }])
        self.assert_exec_cmd(new, 'CLUSTER MEET', 'host0', 6000)
        self.assert_exec_cmd(new,
            'CLUSTER REPLICATE', self.cluster.nodes[1].name)

    @patch.object(Cluster, 'wait')
    @patch.object(Cluster, '_wait_nodes_updated')
    def test_add_nodes_in_fast_mode(self, m1, m2):
        new = self.new_nodes[0]
        self.cluster.add_slaves([{
            'cluster_node': new,
            'role': 'slave',
            'master': self.cluster.nodes[1].name,
        }], fast_mode=True)
        self.assert_exec_cmd(new, 'CLUSTER MEET', 'host0', 6000)
        self.assert_exec_cmd(new,
            'CLUSTER REPLICATE', self.cluster.nodes[1].name)

    def test_distribute_error(self):
        solver = MaxFlowSolver.from_nodes(
            self.cluster.nodes, self.new_nodes[:2])
        dist = solver.get_distribution()
        self.assertEqual(len(sum(dist['masters'], [])), 3)
        self.assertEqual(len(sum(dist['slaves'], [])), 0)
        self.assertEqual(len(sum(dist['frees'], [])), 2)
        with self.assertRaises(DistributionError):
            solver.distribute_slaves()

    def test_distribute(self):
        solver = MaxFlowSolver.from_nodes(
            self.cluster.nodes, self.new_nodes)
        dist = solver.get_distribution()
        self.assertEqual(len(sum(dist['masters'], [])), 3)
        self.assertEqual(len(sum(dist['slaves'], [])), 0)
        self.assertEqual(len(sum(dist['frees'], [])), 3)
        result, frees = solver.distribute_slaves()
        self.assertEqual(len(frees), 0)
        self.assertEqual(len(result), 3)
