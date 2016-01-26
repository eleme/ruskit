import mock
import redis


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


def test_distribute(monkeypatch):
    from ruskit import cluster
    monkeypatch.setattr(cluster, 'ClusterNode', MockNode)

    from ruskit.cmds.create import Manager

    instance = [
        '10.0.15.59:7101',
        '10.0.15.60:7101',
        '10.0.50.139:7101',
        '10.0.15.59:7102',
        '10.0.15.60:7102',
        '10.0.50.139:7102',
    ]

    manager = Manager(1, instance)
    manager.init_slots()

    master_map = {m.name: m for m in manager.masters}

    for s in manager.slaves:
        assert master_map[s.unassigned_master].host != s.host
