import unittest

from mock import MagicMock, patch, call

from ruskit.cluster import ClusterNode, Cluster


CLUSTER_NODES_RESP = \
    'e925e492d37b4ac6125da32ad681896fdff7e7b3 host0:6000 ' \
    '{}master - 0 0 1 connected 0-5461\n'                  \
    'ac4f2168f6ebd97aa54412260d27d3a49dd5eb8a host1:6001 ' \
    '{}master - 0 1469498603296 2 connected 5462-10922\n'  \
    '81b76b0961fda365771f1952ab5ff2a2898fc45c host2:6002 ' \
    '{}master - 0 1469498602285 3 connected 10923-16383\n'


NEW_NODE_RESP_LIST = {
    6003: '70bb37e2f98a5d12d9c39aeafc4528841ba2c0a5 host3:6003 ' \
          'myself,master - 0 0 0 connected',
    6004: 'd390567fd3e66d6a5f8ee44f70994243cb9e1e7f host4:6004 ' \
          'myself,master - 0 0 0 connected',
    6005: '3a59ed121ad67fecf08c5eaa159f98ca3b10926a host5:6005 ' \
          'myself,master - 0 0 0 connected',
}


ROLE_OUTPUT = '*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n' \
              ':6000\r\n$9\r\nconnected\r\n:911\r\n'


class MockRedisClient(object):
    def __init__(self, cluster_node):
        self.execute_command = MagicMock(side_effect=self.side_effect)
        self.mock_redis = MagicMock()
        self.cluster_node = cluster_node

    def __getattr__(self, name):
        if name == 'info':
            return self.gen_defered_func(name)
        return getattr(self.mock_redis, name)

    def side_effect(self, *args, **kwargs):
        return MagicMock()

    def gen_defered_func(self, name):
        command = name.replace('_', ' ').upper()
        def _defered_func(*args, **kwargs):
            return self.execute_command(command, *args, **kwargs)
        return _defered_func


class MockMember(MockRedisClient):
    def side_effect(self, *args, **kwargs):
        args = [a.upper() for a in args if isinstance(a, basestring)]
        if args[0] == 'CLUSTER NODES':
            param = {
                6000: ('myself,', '', ''),
                6001: ('', 'myself,', ''),
                6002: ('', '', 'myself,'),
            }
            return CLUSTER_NODES_RESP.format(
                *param[self.cluster_node.port])
        if args[0] == 'INFO':
            return {
                'cluster_enabled': True,
                'cluster_known_nodes': 3,
                'redis_version': '3.0.3'
            }
        return MagicMock()


class MockNewNode(MockRedisClient):
    def side_effect(self, *args, **kwargs):
        args = [a.upper() for a in args if isinstance(a, basestring)]
        if args[0] == 'CLUSTER NODES':
            return NEW_NODE_RESP_LIST[self.cluster_node.port]
        if args[0] == 'INFO':
            # only for new nodes
            return {
                'cluster_enabled': True,
                'cluster_known_nodes': 0,
                'redis_version': '3.0.3',
            }
        if args[0] == 'CLUSTER INFO':
            return 'cluster_known_nodes:1'
        if args[0] == 'ROLE':
            return ['slave', '127.0.0.1', 6000, 'connected', 2521]
        return MagicMock()


def patch_not_used(func):
    @patch('socket.gethostbyname', lambda h: h)
    def _wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return _wrapper


class TestCaseBase(unittest.TestCase):

    @patch_not_used
    def setUp(self):
        uris = ['host{}:{}'.format(i, p) \
            for i, p in enumerate(range(6000,6006))]
        nodes = [ClusterNode.from_uri(u) for u in uris[:3]]
        for n in nodes:
            n.r = MockMember(n)
        self.cluster = Cluster(nodes)
        self.new_nodes = [ClusterNode.from_uri(u) for u in uris[3:]]
        for n in self.new_nodes:
            n.r = MockNewNode(n)

    def assert_exec_cmd(self, node, *args, **kwargs):
        node.r.execute_command.assert_any_call(*args, **kwargs)

    def assert_no_exec(self, node, *args, **kwargs):
        func = node.r.execute_command
        self.assertNotIn(call(*args, **kwargs), func.mock_calls)

    def assert_not_called_with(self, mock_func, *args, **kwargs):
        self.assertNotIn(call(*args, **kwargs), mock_func.mock_calls)
