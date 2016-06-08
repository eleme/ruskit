import hashlib
import itertools
import redis
import socket
import time
import logging

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from .utils import echo, divide

CLUSTER_HASH_SLOTS = 16384

logger = logging.getLogger(__name__)


def _scan_keys(node, slot, count=10):
    while True:
        keys = node.getkeysinslot(slot, count)
        if not keys:
            break
        for key in keys:
            yield key


class RuskitException(Exception):
    pass


class NodeNotFound(RuskitException):
    def __init__(self, node_id):
        self.node_id = node_id

    def __str__(self):
        return "Node '{}' not found.".format(self.node_id)


class ClusterNotHealthy(RuskitException):
    pass


class ClusterNode(object):
    def __init__(self, host, port, socket_timeout=1, retry=10):
        self.host = socket.gethostbyname(host)
        self.port = port
        self.retry = retry
        self.r = redis.Redis(host, port, socket_timeout=socket_timeout)
        self._cached_node_info = None

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
        return getattr(self.r, attr)

    def execute_command(self, *args, **kwargs):
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

    def nodes(self):
        info = self.execute_command("CLUSTER NODES").strip()
        return self._parse_node(info)

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
                "addr": confs[1],
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


class Cluster(object):
    def __init__(self, nodes):
        self.nodes = nodes

    @classmethod
    def from_node(cls, node):
        nodes = [ClusterNode.from_info(i) for i in node.nodes()
                 if i["link_status"] != "disconnected"]
        return cls(nodes)

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
            slots, names = [], []
            for node in nodes:
                slots.extend(node["slots"])
                names.append(node["name"])
            info = "{}:{}".format('|'.join(sorted(names)),
                                  ','.join(str(i) for i in sorted(slots)))
            sig.add(hashlib.md5(info).hexdigest())
        return len(sig) == 1

    def healthy(self):
        slots = list(itertools.chain(*[i.slots for i in self.nodes]))
        return len(slots) == CLUSTER_HASH_SLOTS and self.consistent()

    def wait(self):
        while not self.consistent():
            time.sleep(1)

        if not self.healthy():
            raise ClusterNotHealthy("Error: missing slots")

    def get_node(self, node_id):
        for i in self.nodes:
            if i.name == node_id:
                return i

    def fix_open_slots(self):
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

        for slot, target_id in info["importing"].items():
            src = self.get_node(target_id)
            if not src or slot not in src.node_info["migrating"]:
                node.setslot("STABLE", slot)
                continue

            self.migrate_slot(src, node, slot)

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
        if node.is_master():
            self.migrate_node(node)

        self.nodes = [n for n in self.nodes if n.name != node.name]
        masters = self.masters
        masters.sort(key=lambda x: len(x.slaves(x.name)))

        for n in self.nodes:
            if n.is_slave(node.name):
                n.replicate(masters[0].name)
            n.forget(node.name)

        assert not node.slots
        node.reset()

    def add_node(self, node):
        """Add a node to cluster.

        :param node: should be formated like this
        `{"addr": "", "role": "slave", "master": "master_node_id"}
        """
        new = ClusterNode.from_uri(node["addr"])
        cluster_member = self.nodes[0]

        new_node_version = new.info()['redis_version']
        cluster_version = cluster_member.info()['redis_version']
        if cluster_version != new_node_version:
            raise RuskitException(
                'invalid redis version, cluster: {}, node: {}'.format(
                    cluster_version, new_node_version))

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
        dst.setslot("IMPORTING", slot, src.name)
        src.setslot("MIGRATING", slot, dst.name)
        for key in _scan_keys(src, slot):
            if verbose:
                echo("Migrating:", key)
            src.migrate(dst.host, dst.port, key, 0, timeout)

        for node in self.masters:
            node.setslot("NODE", slot, dst.name)

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
