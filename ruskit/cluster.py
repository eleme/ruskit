import hashlib
import itertools
import redis
import socket
import time
import logging
import functools
from collections import defaultdict

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from .utils import echo, divide, check_new_nodes, RuskitException

CLUSTER_HASH_SLOTS = 16384
BUSY_MAX_RETRY_TIMES = 10
BUSY_SLEEP_SECONDS = 3


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _scan_keys(node, slot, count=10):
    while True:
        keys = node.getkeysinslot(slot, count)
        if not keys:
            break
        for key in keys:
            yield key


def retry_when_busy_loading(func):
    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        retry = 0
        while True:
            try:
                return func(*args, **kwargs)
            except redis.BusyLoadingError as e:
                if retry >= BUSY_MAX_RETRY_TIMES:
                    raise
                logger.warning(e)
                retry += 1
                time.sleep(BUSY_SLEEP_SECONDS)
    return _wrapper


class NodeNotFound(RuskitException):
    def __init__(self, node_id):
        self.node_id = node_id

    def __str__(self):
        return "Node '{}' not found.".format(self.node_id)


class ClusterNotHealthy(RuskitException):
    pass


class ClusterNode(object):
    socket_timeout = 1
    before_request_redis = None

    def __init__(self, host, port, socket_timeout=None, retry=10):
        socket_timeout = socket_timeout or ClusterNode.socket_timeout
        self.host = socket.gethostbyname(host)
        self.port = port
        self.retry = retry
        self.r = redis.Redis(host, port, socket_timeout=socket_timeout)
        self._cached_node_info = None
        self._cached_nodes = None

    def gen_addr(self):
        return '{}:{}'.format(self.host, self.port)

    @classmethod
    def from_uri(cls, uri):
        if not uri.startswith("redis://"):
            uri = "redis://{}".format(uri)
        d = urlparse.urlparse(uri)
        return cls(d.hostname, d.port)

    @classmethod
    def from_info(cls, info):
        node = cls.from_uri(info["addr"])
        node._cached_node_info = info
        return node

    def __repr__(self):
        return "ClusterNode<{}:{}>".format(self.host, self.port)

    def __getattr__(self, attr):
        if self.before_request_redis:
            self.before_request_redis()
        return getattr(self.r, attr)

    def execute_command(self, *args, **kwargs):
        if self.before_request_redis:
            self.before_request_redis()

        i = 0
        while True:
            try:
                return self.r.execute_command(*args, **kwargs)
            except redis.RedisError:
                if i > self.retry:
                    raise
                i += 1
                logger.warn("retry %d times", i)
                time.sleep(1)

    def is_slave(self, master_id=None):
        info = self.node_info

        r = "slave" in info["flags"]
        if master_id is not None:
            r = r and info["replicate"] == master_id
        return r

    def is_master(self):
        return "master" in self.node_info["flags"]

    @property
    def node_info(self):
        if self._cached_node_info is None:
            self._cached_node_info = self.nodes()[0]
        return self._cached_node_info

    @property
    def slots(self):
        return self.node_info["slots"]

    def flush_cache(self):
        self._cached_nodes = None
        self._cached_node_info = None

    @property
    def name(self):
        return self.node_info["name"]

    def migrate(self, host, port, key, destination_db, timeout, copy=False,
                replace=False):
        args = []
        if copy:
            args = ["COPY"]
        if replace:
            args = ["REPLACE"]
        return self.execute_command("MIGRATE", host, port, key,
                                    destination_db, timeout, *args)

    def reset(self, hard=False, soft=False):
        args = []
        if hard:
            args = ["HARD"]
        if soft:
            args = ["SOFT"]
        return self.execute_command("CLUSTER RESET", *args)

    def setslot(self, action, slot, node_id=None):
        remain = [node_id] if node_id else []
        return self.execute_command("CLUSTER SETSLOT", slot, action, *remain)

    def getkeysinslot(self, slot, count):
        return self.execute_command("CLUSTER GETKEYSINSLOT", slot, count)

    def countkeysinslot(self, slot):
        return self.execute_command("CLUSTER COUNTKEYSINSLOT", slot)

    def slaves(self, node_id):
        data = self.execute_command("CLUSTER SLAVES", node_id)
        return self._parse_node('\n'.join(data))

    def addslots(self, *slot):
        if not slot:
            return

        self.execute_command("CLUSTER ADDSLOTS", *slot)

    def delslots(self, *slot):
        if not slot:
            return

        self.execute_command("CLUSTER DELSLOTS", *slot)

    def forget(self, node_id):
        return self.execute_command("CLUSTER FORGET", node_id)

    def set_config_epoch(self, config_epoch):
        return self.execute_command("CLUSTER SET-CONFIG-EPOCH", config_epoch)

    def meet(self, ip, port):
        return self.execute_command("CLUSTER MEET", ip, port)

    def replicate(self, node_id):
        return self.execute_command("CLUSTER REPLICATE", node_id)

    def failover(self, force=False, takeover=False):
        args = ["FORCE"] if force else ["TAKEOVER"]
        return self.execute_command("CLUSTER FAILOVER", *args)

    @retry_when_busy_loading
    def nodes(self):
        info = self.execute_command("CLUSTER NODES").strip()
        return self._parse_node(info)

    def nodes_with_cache(self):
        if self._cached_nodes is None:
            self._cached_nodes = self.nodes()
        return self._cached_nodes

    def cluster_info(self):
        data = {}
        info = self.execute_command("CLUSTER INFO").strip()
        for item in info.split("\r\n"):
            k, v = item.split(':')
            if k != "cluster_state":
                v = int(v)
            data[k] = v
        return data

    def _parse_node(self, nodes):
        data = []
        for item in nodes.split('\n'):
            if not item:
                continue
            confs = item.split()
            node_info = {
                "name": confs[0],
                "addr": confs[1].split("@")[0],
                "flags": confs[2].split(','),
                "replicate": confs[3],  # master_id
                "ping_sent": int(confs[4]),
                "ping_recv": int(confs[5]),
                "link_status": confs[7],
                "migrating": {},
                "importing": {},
                "slots": []
            }
            for slot in confs[8:]:
                if slot[0] == '[':
                    if "->-" in slot:
                        s, dst = slot[1:-1].split("->-")
                        node_info["migrating"][s] = dst
                    elif "-<-" in slot:
                        s, src = slot[1:-1].split("-<-")
                        node_info["importing"][s] = src
                elif '-' in slot:
                    start, end = slot.split('-')
                    node_info["slots"].extend(range(int(start), int(end) + 1))
                else:
                    node_info["slots"].append(int(slot))

            if "myself" in node_info["flags"]:
                data.insert(0, node_info)
            else:
                data.append(node_info)
        return data


class ActionStopped(Exception):
    pass


class Cluster(object):
    def __init__(self, nodes):
        self.nodes = nodes
        self.check_action_stopped = lambda: False

    def set_stop_checking_hook(self, hook):
        self.check_action_stopped = hook

    @classmethod
    def from_node(cls, node):
        nodes = [ClusterNode.from_info(i) for i in node.nodes()
                 if i["link_status"] != "disconnected"]
        return cls(nodes)

    def flush_all_cache(self):
        for n in self.nodes:
            n.flush_cache()

    def get_slow_logs(self):
        result = {}
        for master in self.masters:
            result[master] = master.slowlog_get(128)
        return result

    @property
    def masters(self):
        return [i for i in self.nodes if i.is_master()]

    def consistent(self):
        sig = set()
        for instance in self.nodes:
            if not instance.is_master():
                continue
            nodes = instance.nodes()
            slots_map = sorted((n['name'], sorted(n['slots'])) for n in nodes)
            sig.add(hashlib.md5(str(slots_map)).hexdigest())
        return len(sig) == 1

    def healthy(self):
        self.flush_all_cache()
        slots = list(itertools.chain(*[i.slots for i in self.nodes]))
        return len(slots) == CLUSTER_HASH_SLOTS and self.consistent()

    @retry_when_busy_loading
    def wait(self):
        start = time.time()
        while not self.consistent():
            time.sleep(1)
        logger.info('cluster took {} seconds to become consistent'.format(
            time.time() - start))

        if not self.healthy():
            raise ClusterNotHealthy("Error: missing slots")

    def get_node(self, node_id):
        for i in self.nodes:
            if i.name == node_id:
                return i

    def fix_open_slots(self):
        self.flush_all_cache()
        for master in self.masters:
            self.fix_node(master)

    def fix_node(self, node):
        info = node.node_info

        for slot, target_id in info["migrating"].items():
            target = self.get_node(target_id)
            if not target or slot not in target.node_info["importing"]:
                node.setslot("STABLE", slot)
                continue

            self.migrate_slot(node, target, slot)
            target.flush_cache()

        for slot, target_id in info["importing"].items():
            src = self.get_node(target_id)
            if not src or slot not in src.node_info["migrating"]:
                node.setslot("STABLE", slot)
                continue

            self.migrate_slot(src, node, slot)
            src.flush_cache()

        node.flush_cache()

    def reshard(self):
        if not self.consistent():
            return

        nodes = [{
            "node": n,
            "count": len(n.slots),
            "need": []
        } for n in self.masters]

        nodes = slot_balance(nodes, CLUSTER_HASH_SLOTS)

        for n in nodes:
            if not n["need"]:
                continue
            for src, count in n["need"]:
                self.migrate(src, n["node"], count)

    def delete_node(self, node):
        node.flush_cache()
        self.flush_all_cache()

        if node.is_master():
            self.migrate_node(node)

        self.nodes = [n for n in self.nodes if n.name != node.name]
        masters = self.masters
        masters.sort(key=lambda x: len(x.slaves(x.name)))

        for n in self.nodes:
            if n.is_slave(node.name):
                n.replicate(masters[0].name)
            n.forget(node.name)

        self.flush_all_cache()

        assert not node.slots
        node.reset()

    def add_nodes(self, nodes):
        '''The format of node is similar to add_node.
        If node contains a key 'cluster_node'
        whose value is an instance of ClusterNode,
        it will be used directly.
        '''
        new_nodes, master_map = self._add_nodes_as_master(nodes)

        for n in new_nodes:
            if n.name not in master_map:
                continue
            master_name = master_map[n.name]
            target = self.get_node(master_name)
            if not target:
                raise NodeNotFound(master_name)
            n.replicate(target.name)
            n.flush_cache()
            target.flush_cache()

    def add_slaves(self, new_slaves, fast_mode=False):
        '''This is almost the same with `add_nodes`. The difference is that
        it will add slaves in two slower way.
        This is mainly used to avoid huge overhead caused by full sync
        when large amount of slaves are added to cluster.
        If fast_mode is False, there is only one master node doing
        replication at the same time.
        If fast_mode is True, only after the current slave has finshed
        its sync, will the next slave on the same host start replication.
        '''
        new_nodes, master_map = self._prepare_for_adding(new_slaves)

        slaves = defaultdict(list)
        for s in new_nodes:
            slaves[s.host].append(s)

        waiting = set()
        while len(slaves) > 0:
            for host in slaves.keys():
                if self.check_action_stopped():
                    logger.warning('Slaves adding was stopped, ' \
                        'waiting for the last slave to be finished')
                slave_list = slaves[host]
                s = slave_list[0]

                if not fast_mode and len(waiting) > 0 and s not in waiting:
                    continue

                if s not in waiting:
                    if self.check_action_stopped():
                        raise ActionStopped(
                            'Slaves adding was successfully stopped')
                    master_name = master_map[s.name]
                    target = self.get_node(master_name)
                    if not target:
                        raise NodeNotFound(master_name)
                    self._add_as_master(s, self.nodes[0])
                    s.replicate(target.name)
                    waiting.add(s)
                    continue

                role = s.execute_command('ROLE')
                # role[3] being changed to 'connected' means sync is finished
                if role[0] == 'master' or role[3] != 'connected':
                    continue

                waiting.remove(s)
                slave_list.pop(0)
                if len(slave_list) == 0:
                    slaves.pop(host)

            if len(waiting) > 0:
                waiting_list = ['{}:{}'.format(n.host, n.port) for n in waiting]
                logger.info('sync waiting list: {}'.format(waiting_list))
            time.sleep(1)

    def _prepare_for_adding(self, nodes):
        assert all(map(
            lambda n: n['role'] == 'master' or 'master' in n, nodes))
        new_nodes = [n.get('cluster_node') or ClusterNode.from_uri(n['addr']) \
            for n in nodes]
        master_map = {a.name: b['master'] \
            for a, b in zip(new_nodes, nodes) if b.get('master')}
        cluster_member = self.nodes[0]
        check_new_nodes(new_nodes, [cluster_member])
        return new_nodes, master_map

    def _add_as_master(self, new_node, cluster_member):
        new_node.meet(cluster_member.host, cluster_member.port)
        self._wait_nodes_updated(cluster_member, [new_node])
        self.wait()
        self.nodes.append(new_node)

    def _add_nodes_as_master(self, nodes):
        new_nodes, master_map = self._prepare_for_adding(nodes)
        cluster_member = self.nodes[0]

        for n in new_nodes:
            n.meet(cluster_member.host, cluster_member.port)
        self._wait_nodes_updated(cluster_member, new_nodes)
        self.wait()
        self.nodes.extend(new_nodes)

        return new_nodes, master_map

    def _wait_nodes_updated(self, cluster_member, new_nodes):
        '''Sometimes even cluster_member has responded new_node.meet,
        cluster_memeber.nodes() are still not updated
        '''
        while True:
            known_nodes = [n['name'] for n in cluster_member.nodes()]
            added_nodes = [n.name for n in new_nodes]
            if len(set(known_nodes) & set(added_nodes)) == len(added_nodes):
                break;
            logger.info('waiting for adding new nodes in `cluster nodes`')
            time.sleep(1)

    def add_node(self, node):
        """Add a node to cluster.

        :param node: should be formated like this
        `{"addr": "", "role": "slave", "master": "master_node_id"}
        """
        new = ClusterNode.from_uri(node["addr"])
        cluster_member = self.nodes[0]
        check_new_nodes([new], [cluster_member])

        new.meet(cluster_member.host, cluster_member.port)
        self.nodes.append(new)

        self.wait()

        if node["role"] != "slave":
            return

        if "master" in node:
            target = self.get_node(node["master"])
            if not target:
                raise NodeNotFound(node["master"])
        else:
            masters = sorted(self.masters, key=lambda x: len(x.slaves(x.name)))
            target = masters[0]

        new.replicate(target.name)
        new.flush_cache()
        target.flush_cache()

    def fill_slots(self):
        masters = self.masters
        slots = itertools.chain(*[n.slots for n in masters])
        missing = list(set(range(CLUSTER_HASH_SLOTS)).difference(slots))

        div = divide(len(missing), len(masters))
        masters.sort(key=lambda x: len(x.slots))

        i = 0
        for count, node in zip(div, masters):
            node.addslots(*missing[i:count + i])
            i += count
            node.flush_cache()

    def migrate_node(self, src_node, count=None, income=False):
        nodes = [n for n in self.masters if n.name != src_node.name]
        slot_count = len(src_node.slots)
        if count is None or count > slot_count:
            count = slot_count

        if count <= 0:
            return
        slots = divide(count, len(nodes))

        reverse = True if income else False
        nodes.sort(key=lambda x: len(x.slots), reverse=reverse)

        for node, count in zip(nodes, slots):
            src, dst = (node, src_node) if income else (src_node, node)
            self.migrate(src, dst, count)

    def migrate_slot(self, src, dst, slot, timeout=15000, verbose=True):
        if self.check_action_stopped():
            raise ActionStopped('Slot migration was successfully stopped')

        dst.setslot("IMPORTING", slot, src.name)
        src.setslot("MIGRATING", slot, dst.name)
        for key in _scan_keys(src, slot):
            if verbose:
                echo("Migrating:", key)
            src.migrate(dst.host, dst.port, key, 0, timeout)

        for node in self.masters:
            node.setslot("NODE", slot, dst.name)
            node.flush_cache()

    def migrate(self, src, dst, count, verbose=True):
        if count <= 0:
            return

        slots = src.slots
        slots_count = len(slots)
        if count > slots_count:
            count = slots_count

        keys = [(s, src.countkeysinslot(s)) for s in slots]
        keys.sort(key=lambda x: x[1])

        for slot, _ in keys[:count]:
            self.migrate_slot(src, dst, slot, verbose=verbose)
        src.flush_cache()
        dst.flush_cache()


def slot_balance(seq, amt):
    seq.sort(key=lambda x: x["count"], reverse=True)
    chunks = divide(amt, len(seq))
    pairs = list(zip(seq, chunks))

    i, j = 0, len(pairs) - 1
    while i < j:
        m, count = pairs[i]
        more = m["count"] - count
        if more <= 0:
            i += 1
            continue

        n, count = pairs[j]
        need = count - n["count"]
        if need <= 0:
            j -= 1
            continue

        if need < more:
            n["need"].append((m["node"], need))
            n["count"] += need
            m["count"] -= need
            j -= 1
        elif need > more:
            n["need"].append((m["node"], more))
            n["count"] += more
            m["count"] -= more
            i += 1
        else:
            n["need"].append((m["node"], need))
            n["count"] += need
            m["count"] -= more
            j -= 1
            i += 1

    return seq
