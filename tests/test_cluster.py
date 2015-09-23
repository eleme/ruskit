import mock

from ruskit.cluster import Cluster


class MockNode(object):
    def __init__(self, name, slots, role):
        self.name = name
        self.slots = slots
        self.role = role

    def is_master(self):
        return self.role == "master"


def test_migrate_node():
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
