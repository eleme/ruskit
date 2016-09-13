import mock
import redis
from mock import patch

from test_base import TestCaseBase
from ruskit.cluster import Cluster, ClusterNode


class MockNode(object):
    def __init__(self, name, slots, role, host='', port=0):
        self.name = name
        self.slots = slots
        self.role = role
        self.host = host
        self.port = port

    def is_master(self):
        return self.role == "master"

    @classmethod
    def from_uri(cls, i):
        host, port = i.split(':')
        c = cls('', 0, '', host, int(port))
        c.name = i
        return c


def test_migrate_node():
    from ruskit.cluster import Cluster

    node1 = MockNode("node1", list(range(3)), "master")
    node2 = MockNode("node2", list(range(2, 5)), "master")
    node3 = MockNode("node3", list(range(5, 8)), "master")

    cluster = Cluster([node1, node2, node3])

    with mock.patch.object(cluster, "migrate") as mock_migrate:
        cluster.migrate_node(node2)

    calls = [
        mock.call(node2, node1, 2),
        mock.call(node2, node3, 1)
    ]
    mock_migrate.assert_has_calls(calls)


def test_migrate_node_with_count():
    from ruskit.cluster import Cluster

    node1 = MockNode("node1", list(range(3)), "master")
    node2 = MockNode("node2", list(range(2, 8)), "master")
    node3 = MockNode("node3", list(range(8, 12)), "master")

    cluster = Cluster([node1, node2, node3])

    with mock.patch.object(cluster, "migrate") as mock_migrate:
        cluster.migrate_node(node2, count=5)

    calls = [
        mock.call(node2, node1, 3),
        mock.call(node2, node3, 2)
    ]
    mock_migrate.assert_has_calls(calls)


def test_migrate_node_with_income():
    from ruskit.cluster import Cluster

    node1 = MockNode("node1", list(range(3)), "master")
    node2 = MockNode("node2", list(range(2, 5)), "master")
    node3 = MockNode("node3", list(range(5, 10)), "master")

    cluster = Cluster([node1, node2, node3])

    with mock.patch.object(cluster, "migrate") as mock_migrate:
        cluster.migrate_node(node2, income=True)

    calls = [
        mock.call(node3, node2, 2),
        mock.call(node1, node2, 1)
    ]
    mock_migrate.assert_has_calls(calls)


def test_retry(monkeypatch):
    from ruskit.cluster import ClusterNode

    monkeypatch.setattr(redis.Redis, "ping", mock.Mock())
    times = [0]

    def execute(*args, **kwargs):
        times[0] += 1
        if times[0] < 4:
            raise redis.RedisError('Err: connection timeout')

    monkeypatch.setattr(redis.Redis, "execute_command", execute)

    node = ClusterNode("localhost", 8000, retry=3)
    node.setslot('NODE', 844, 'abcd')

    assert times[0] == 4


@patch('ruskit.cmds.create.ClusterNode', MockNode)
def test_distribute(monkeypatch):
    from ruskit import cluster
    from ruskit.cmds.create import Manager

    instance = [
        'host1:1',
        'host2:2',
        'host3:3',
        'host4:4',
        'host5:5',
        'host6:6',
    ]

    manager = Manager(1, instance)
    manager.init_slots()

    master_map = {m.name: m for m in manager.masters}

    for s in manager.slaves:
        assert master_map[s.unassigned_master].host != s.host


class TestCluster(TestCaseBase):

    def clear_slots(node):
            node._cached_node_info['slots'] = []
            return mock.MagicMock()

    @patch.object(Cluster, 'migrate_node', side_effect=clear_slots)
    def test_delete(self, migrate_node):
        cluster = self.cluster
        a = cluster.nodes[0]
        b = cluster.nodes[1]
        c = cluster.nodes[2]
        cluster.delete_node(a)
        migrate_node.assert_called_with(a)
        self.assertEqual(cluster.get_node(a.name), None)
        self.assert_exec_cmd(a, 'CLUSTER RESET')
        self.assert_exec_cmd(b, 'CLUSTER FORGET', a.name)
        self.assert_exec_cmd(c, 'CLUSTER FORGET', a.name)

    @patch.object(Cluster, 'migrate_slot')
    def test_fix_node_migrating(self, migrate_slot):
        cluster = self.cluster
        a = cluster.nodes[0]
        b = cluster.nodes[1]
        c = cluster.nodes[2]
        for n in cluster.nodes:
            n.node_info  # gen node_info
        a._cached_node_info['migrating'] = {
            '233': b.name,
            '666': c.name,
            '99': 'name_not_in_cluster',
        }
        b._cached_node_info['importing'] = {'233': a.name}
        cluster.fix_node(a)
        self.assert_no_exec(a, 'CLUSTER SETSLOT', '233', 'STABLE')
        self.assert_no_exec(b, 'CLUSTER SETSLOT', '233', 'STABLE')
        self.assert_exec_cmd(a, 'CLUSTER SETSLOT', '666', 'STABLE')
        self.assert_exec_cmd(a, 'CLUSTER SETSLOT', '99', 'STABLE')
        migrate_slot.assert_called_with(a, b, '233')
        self.assert_not_called_with(a, c, '666')

    @patch.object(Cluster, 'migrate_slot')
    def test_fix_node_importing(self, migrate_slot):
        cluster = self.cluster
        a = cluster.nodes[0]
        b = cluster.nodes[1]
        c = cluster.nodes[2]
        for n in cluster.nodes:
            n.node_info  # gen node_info
        a._cached_node_info['importing'] = {
            '233': b.name,
            '666': c.name,
            '99': 'name_not_in_cluster',
        }
        b._cached_node_info['migrating'] = {'233': a.name}
        cluster.fix_node(a)
        self.assert_no_exec(a, 'CLUSTER SETSLOT', '233', 'STABLE')
        self.assert_no_exec(b, 'CLUSTER SETSLOT', '233', 'STABLE')
        self.assert_exec_cmd(a, 'CLUSTER SETSLOT', '666', 'STABLE')
        self.assert_exec_cmd(a, 'CLUSTER SETSLOT', '99', 'STABLE')
        migrate_slot.assert_called_with(b, a, '233')
        self.assert_not_called_with(migrate_slot, c, a, '666')

    def test_fill_slots(self):
        cluster = self.cluster
        a = cluster.nodes[0]
        b = cluster.nodes[1]
        c = cluster.nodes[2]
        for n in cluster.nodes:
            n.node_info  # gen node_info
        missing_slots = a._cached_node_info['slots'][-6:]
        a._cached_node_info['slots'] = a._cached_node_info['slots'][:-6]
        cluster.fill_slots()
        added_slots = []
        for n in cluster.nodes:
            added_slots.extend(
                list(args[1:]) for args, kwargs \
                in n.r.execute_command.call_args_list \
                if args[0] == 'CLUSTER ADDSLOTS')
        added_slots = sum(added_slots, [])
        self.assertEqual(set(added_slots), set(missing_slots))

    def test_consistent(self):
        self.assertTrue(self.cluster.consistent())

    def gen_nodes_method():
        backup_func = ClusterNode.nodes
        def inconsistent_slots(node):
            result = backup_func(node)
            if node.port == 6000:
                slot = result[1]['slots'].pop()
                result[0]['slots'].append(slot)
            return result
        return inconsistent_slots

    @patch.object(ClusterNode, 'nodes', new_callable=gen_nodes_method)
    def test_slots_consistent(self, _):
        self.assertFalse(self.cluster.consistent())

    def test_hook(self):
        node = self.cluster.nodes[0]
        m1, m2 = mock.Mock(), mock.Mock()
        ClusterNode.before_request_redis = m1
        node.ping()
        m1.assert_called()
        ClusterNode.before_request_redis = m2
        node.nodes()
        m2.assert_called()
        ClusterNode.before_request_redis = None
