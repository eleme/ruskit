from mock import patch

from ruskit.cluster import ClusterNode, Cluster
from ruskit.health import NodeListChecker, NameChecker, RoleChecker, \
    ConnectChecker, SlotChecker, ReplicateChecker, FailFlagChecker
from test_base import TestCaseBase, MockMember


NODES1 = \
    'e925e492d37b4ac6125da32ad681896fdff7e7b3 host0:6000 '   \
    '{}master - 0 0 1 connected 0-5000\n'                    \
    'ac4f2168f6ebd97aa54412260d27d3a49dd5eb8a host1:6001 '   \
    '{}master - 0 1469498603296 2 disconnected 5462-10922\n' \
    '81b76b0961fda365771f1952ab5ff2a2898fc45c host2:6002 '   \
    '{}master - 0 1469498602285 3 connected 10923-16383\n'   \
    '3a59ed121ad67fecf08c5eaa159f98ca3b10926a host5:7001 '   \
    'slave e925e492d37b4ac6125da32ad681896fdff7e7b3 0 0 0 connected'
NODES2 = \
    'e925e492d37b4ac6125da32ad681896fdff7e7b3 host0:6000 '    \
    '{}slave 81b76b0961fda365771f1952ab5ff2a2898fc45c 0 0 1 ' \
    'connected 0-5461\n'                                      \
    'ac4f2168f6ebd97aa54412260d27d3a49dd5eb8a host1:6001 '    \
    '{}master - 0 1469498603296 2 connected 5462-10922\n'     \
    '3a59ed121ad67fecf08c5eaa159f98ca3b10926a host5:7001 '    \
    'slave,fail? 81b76b0961fda365771f1952ab5ff2a2898fc45c 0 ' \
    '1469498603296 2 connected\n'                             \
    '81b76b0961fda365771f1952ab5ff2a2898fc45c host2:6002 '    \
    '{}master - 0 1469498602285 3 connected 10923-16383\n'
NODES3 = \
    'e925e492d37b4ac6125da32ad681896fdff7e7b3 host0:6000 ' \
    '{}master - 0 0 1 connected 0-5461\n'                  \
    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa host1:7002 ' \
    '{}master - 0 1469498603296 2 connected 5462-10922\n'  \
    '2333333333333333333333333333333333333333 host2:6002 ' \
    '{}master - 0 1469498602285 3 connected 10923-16383\n'


class TestHealthCheck(TestCaseBase):

    def setUp(self):
        super(TestHealthCheck, self).setUp()
        self.all_addrs = [
            'host0:6000', 'host1:6001', 'host2:6002', 'host5:7001',
            'host1:7002']
        nodes = self.gen_nodes()
        nodes[0].r.cluster_node_resp = NODES1
        nodes[1].r.cluster_node_resp = NODES2
        nodes[2].r.cluster_node_resp = NODES3
        self.nodes = nodes

    def test_node_list_check(self):
        checker = NodeListChecker(self.nodes, self.all_addrs)
        report = checker.check()

        self.assertEqual(len(report), 2)
        diff = self.get_diff_by_addr('host0:6000', report)
        self.assertEqual(diff, self.get_diff_by_addr('host1:6001', report))
        self.assertEqual(len(diff), 2)
        self.assertNotIn('host0:6000', diff)
        self.assertNotIn('host0:6002', diff)
        self.assertIn('host1:6001', diff)
        self.assertIn('host5:7001', diff)

        diff = self.get_diff_by_addr('host2:6002', report)
        self.assertEqual(len(diff), 1)
        self.assertIn('host1:7002', diff)

    def test_name_check(self):
        checker = NameChecker(self.nodes, self.all_addrs)
        report = checker.check()

        self.assertEqual(len(report), 2)
        diff = self.get_diff_by_addr('host0:6000', report)
        self.assertEqual(diff, self.get_diff_by_addr('host1:6001', report))
        self.assertEqual(diff['host2:6002'],
            '81b76b0961fda365771f1952ab5ff2a2898fc45c')

        diff = self.get_diff_by_addr('host2:6002', report)
        self.assertEqual(diff['host2:6002'],
            '2333333333333333333333333333333333333333')

    def test_role_check(self):
        checker = RoleChecker(self.nodes, self.all_addrs)
        report = checker.check()

        self.assertEqual(len(report), 2)
        diff = self.get_diff_by_addr('host0:6000', report)
        self.assertEqual(diff, self.get_diff_by_addr('host2:6002', report))
        self.assertEqual(diff['host0:6000'], 'master')

        diff = self.get_diff_by_addr('host1:6001', report)
        self.assertEqual(diff['host0:6000'], 'slave')

    def test_connect_check(self):
        checker = ConnectChecker(self.nodes, self.all_addrs)
        report = checker.check()

        self.assertEqual(len(report), 3)
        diff = self.get_diff_by_addr('host0:6000', report)
        self.assertEqual(diff['host1:6001'], 'disconnected')

        diff = self.get_diff_by_addr('host1:6001', report)
        self.assertEqual(diff['host1:6001'], 'connected')

        diff = self.get_diff_by_addr('host2:6002', report)
        self.assertEqual(diff.get('host1:6001'), None)

    def test_slot_check(self):
        checker = SlotChecker(self.nodes, self.all_addrs)
        report = checker.check()

        self.assertEqual(len(report), 2)
        diff = self.get_diff_by_addr('host0:6000', report)
        self.assertEqual(str(diff['host0:6000']), str(range(5001)))

        diff = self.get_diff_by_addr('host1:6001', report)
        self.assertEqual(diff, self.get_diff_by_addr('host2:6002', report))
        self.assertEqual(str(diff['host0:6000']), str(range(5462)))

    def test_replicate_check(self):
        checker = ReplicateChecker(self.nodes, self.all_addrs)
        report = checker.check()

        self.assertEqual(len(report), 3)
        diff = self.get_diff_by_addr('host0:6000', report)
        self.assertEqual(diff['host5:7001'],
            'e925e492d37b4ac6125da32ad681896fdff7e7b3')

        diff = self.get_diff_by_addr('host1:6001', report)
        self.assertEqual(diff['host5:7001'],
            '81b76b0961fda365771f1952ab5ff2a2898fc45c')

        diff = self.get_diff_by_addr('host2:6002', report)
        self.assertEqual(diff.get('host5:7001'), None)

    def test_fail_flag_check(self):
        checker = FailFlagChecker(self.nodes, self.all_addrs)
        report = checker.check()

        self.assertEqual(len(report), 3)
        diff = self.get_diff_by_addr('host0:6000', report)
        self.assertEqual(diff['host5:7001'], 'alive')

        diff = self.get_diff_by_addr('host1:6001', report)
        self.assertEqual(diff['host5:7001'], 'fail?')

        diff = self.get_diff_by_addr('host2:6002', report)
        self.assertEqual(diff.get('host5:7001'), None)

    def get_diff_by_addr(self, addr, report):
        for parts in report:
            if addr in parts['nodes']:
                return parts['diff']
