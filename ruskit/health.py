import hashlib
import redis
from collections import defaultdict

from .cluster import ClusterNode
from .utils import NO_RETRY


class HealthCheckManager(object):
    def __init__(self, nodes):
        self.down_nodes = set()
        all_addrs = set(n.gen_addr() for n in nodes)
        while True:
            former_len = len(all_addrs)
            active_nodes = []
            for n in nodes:
                try:
                    addrs = [node['addr'] for node in n.nodes()]
                    all_addrs.update(addrs)
                    active_nodes.append(n)
                except redis.RedisError:
                    self.down_nodes.add(n.gen_addr())

            nodes = []
            for addr in all_addrs:
                host, port = addr.split(':')
                nodes.append(ClusterNode(host, int(port), retry=NO_RETRY))

            if len(all_addrs) == former_len:
                break

        self.nodes = active_nodes
        self.all_addrs = all_addrs

    def check(self):
        checkers = {
            'node_list': NodeListChecker,
            'name': NameChecker,
            'role': RoleChecker,
            'connect': ConnectChecker,
            'slot': SlotChecker,
            'replicate': ReplicateChecker,
            'fail_flag': FailFlagChecker,
        }
        report = {check_name: Checker(self.nodes, self.all_addrs).check() \
            for check_name, Checker in checkers.iteritems()}
        report = {k: diff for k, diff in report.iteritems() if diff}
        if len(self.down_nodes) > 0:
            report['down_nodes'] = list(self.down_nodes)
        return report if report else None


class Classifier(object):
    def __init__(self, nodes, all_addrs, checker):
        nodes_info = {}
        info_map = {n: checker.gen_info(n) for n in nodes}

        for addr in all_addrs:
            sig = set()
            for n in nodes:
                info = info_map[n].get(addr)
                if info is None and checker.ignore_missing_when_classify:
                    continue
                sig.add(hashlib.md5(str(info)).hexdigest())
            if len(sig) > 1:
                continue
            for info in info_map.values():
                if addr in info:
                    info.pop(addr)

        addrs = set()
        for n, info in info_map.iteritems():
            addrs.update(info.keys())

        for node in nodes:
            info = info_map[node]
            info_hash = self.gen_hash(info)
            if info_hash not in nodes_info:
                nodes_info[info_hash] = NodesInfo([node], info, info_hash)
            else:
                nodes_info[info_hash].nodes.append(node)
        nodes_info_list = nodes_info.values()
        self.find_diff(checker, addrs, nodes_info_list)
        self.nodes_info_list = nodes_info_list
        self.checker = checker

    def gen_hash(self, info):
        info = sorted(info.iteritems())
        return hashlib.md5(str(info)).hexdigest()

    def gen_diff_report(self):
        if all(len(i.diff) == 0 for i in self.nodes_info_list):
            return None

        report = []
        for nodes_info in self.nodes_info_list:
            report.append({
                'nodes': [n.gen_addr() for n in nodes_info.nodes],
                'diff': self.checker.gen_diff_report(nodes_info.diff),
            })
        return report

    def find_diff(self, checker, addrs, nodes_info_list):
        for addr in addrs:
            for nodes_info in nodes_info_list:
                info = nodes_info.info.get(addr)
                if info is None and checker.ignore_missing_in_result:
                    continue
                nodes_info.diff[addr] = info


class NodesInfo(object):
    def __init__(self, nodes, info, info_hash):
        ''' info should be: {
            addr: info_per_node,
            addr: info_per_node,
            ...
        }
        '''
        self.nodes = nodes
        self.info = info
        self.info_hash = info_hash
        self.diff = {}


class HealthChecker(object):
    ignore_missing_when_classify = True
    ignore_missing_in_result = False

    def __init__(self, nodes, all_addrs):
        self.nodes = nodes
        self.all_addrs = all_addrs

    def check(self):
        classifier = Classifier(self.nodes, self.all_addrs, self)
        return classifier.gen_diff_report()

    def gen_diff_report(self, info):
        return info


class NodeListChecker(HealthChecker):
    ignore_missing_when_classify = False
    ignore_missing_in_result = True

    def gen_info(self, node):
        return {n['addr']: True for n in node.nodes_with_cache()}

    def gen_diff_report(self, info):
        return [addr for addr, exists in info.iteritems()]


class NameChecker(HealthChecker):
    def gen_info(self, node):
        return {n['addr']: n['name'] for n in node.nodes_with_cache()}


class RoleChecker(HealthChecker):
    def gen_info(self, node):
        return {n['addr']: 'master' if 'master' in n['flags'] else 'slave' \
            for n in node.nodes_with_cache()}


class ConnectChecker(HealthChecker):
    def gen_info(self, node):
        return {n['addr']: n['link_status'] for n in node.nodes_with_cache()}


class SlotChecker(HealthChecker):
    def gen_info(self, node):
        return {n['addr']: sorted(n['slots']) \
            for n in node.nodes_with_cache()}


class ReplicateChecker(HealthChecker):
    def gen_info(self, node):
        return {n['addr']: n['replicate'] \
            if n['replicate'] != '-' else 'master' \
            for n in node.nodes_with_cache()}


class FailFlagChecker(HealthChecker):
    ignore_missing_when_classify = True
    ignore_missing_in_result = True

    ALIVE_TAG = 'alive'

    def gen_info(self, node):
        return {n['addr']: self.get_fail_flag(n['flags']) \
            for n in node.nodes_with_cache()}

    def get_fail_flag(self, flags):
        if 'fail?' in flags:
            return 'fail?'
        elif 'fail' in flags:
            return 'fail'
        else:
            return self.ALIVE_TAG
