import hashlib
import itertools
import redis
import math
import socket
import time
import urlparse

from .utils import echo, divide_number

CLUSTER_HASH_SLOTS = 16384


def _scan_keys(node, slot, count=10):
    while True:
        keys = node.getkeysinslot(slot, count)
        if not keys:
            break
        for key in keys:
            yield key


class NodeNotFound(Exception):
    def __init__(self, node_id):
        self.node_id = node_id

    def __str__(self):
        return "Node '{}' not found.".format(self.node_id)


class ClusterNode(object):
    def __init__(self, host, port):
        self.host = socket.gethostbyname(host)
        self.port = port
        self.r = redis.Redis(host, port)

    @classmethod
    def from_uri(cls, uri):
        if not uri.startswith("redis://"):
            uri = "redis://{}".format(uri)
        d = urlparse.urlparse(uri)
        return cls(d.hostname, d.port)

    def __repr__(self):
        return "ClusterNode<{}:{}>".format(self.host, self.port)

    def __getattr__(self, attr):
        return getattr(self.r, attr)

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
        nodes = self.nodes()
        return nodes[0]

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
        return self.r.execute_command("MIGRATE", host, port, key,
                                      destination_db, timeout, *args)

    def setslot(self, action, slot, node_id=None):
        remain = [node_id] if node_id else []
        return self.r.execute_command("CLUSTER SETSLOT", slot, action, *remain)

    def getkeysinslot(self, slot, count):
        return self.r.execute_command("CLUSTER GETKEYSINSLOT", slot, count)

    def countkeysinslot(self, slot):
        return self.r.execute_command("CLUSTER COUNTKEYSINSLOT", slot)

    def slaves(self, node_id):
        return self.r.execute_command("CLUSTER SLAVES", node_id)

    def addslots(self, *slot):
        if not slot:
            return

        self.r.execute_command("CLUSTER ADDSLOTS", *slot)

    def delslots(self, *slot):
        if not slot:
            return

        self.r.execute_command("CLUSTER DELSLOTS", *slot)

    def forget(self, node_id):
        return self.r.execute_command("CLUSTER FORGET", node_id)

    def set_config_epoch(self, config_epoch):
        return self.r.execute_command("CLUSTER SET-CONFIG-EPOCH", config_epoch)

    def meet(self, ip, port):
        return self.r.execute_command("CLUSTER MEET", ip, port)

    def replicate(self, node_id):
        return self.r.execute_command("CLUSTER REPLICATE", node_id)

    def failover(self, force=False, takeover=False):
        args = ["FORCE"] if force else ["TAKEOVER"]
        return self.r.execute_command("CLUSTER FAILOVER", *args)

    def nodes(self):
        data = []
        info = self.r.execute_command("CLUSTER NODES").strip()
        for item in info.split('\n'):
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

    def cluster_info(self):
        data = {}
        info = self.r.execute_command("CLUSTER INFO").strip()
        for item in info.split("\r\n"):
            k, v = item.split(':')
            if k != "cluster_state":
                v = int(v)
            data[k] = v
        return data


class Cluster(object):
    def __init__(self, nodes):
        self.nodes = nodes

    @classmethod
    def from_node(cls, node):
        nodes = [ClusterNode.from_uri(i["addr"]) for i in node.nodes()]
        return cls(nodes)

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

    def wait(self, verbose=False):
        while not self.consistent():
            echo('.', end='', disable=verbose)
            time.sleep(1)
        echo(disable=verbose)

        if not self.healthy():
            echo("Error: missing slots", disable=verbose)

    def get_node(self, node_id):
        for i in self.nodes:
            if i.name == node_id:
                return i

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
                break
            for src, count in n["need"]:
                self.migrate(src, n["node"], count)

    def delete_node(self, node):
        self.nodes = [n for n in self.nodes if n.name != node.name]
        if node.is_master():
            self.migrate_node(node)

        masters = self.masters
        masters.sort(key=lambda x: len(x.slaves(x.name)))

        for n in self.nodes:
            if n.is_slave(node.name):
                n.replicate(masters[0].name)
            n.forget(node.name)

        if node.is_slave():
            node.failover(takeover=True)
        node.delslots(*node.slots)

        for n in self.nodes:
            node.forget(n.name)

    def add_node(self, node):
        """Add a node to cluster.

        :param node: should be formated like this
        `{"addr": "", "role": "slave", "master": "master_node_id"}
        """
        new = ClusterNode.from_uri(node["addr"])
        cluster_member = self.nodes[0]
        new.meet(cluster_member.host, cluster_member.port)
        self.nodes.append(new)

        if node["role"] == "slave":
            if "master" in node:
                target = self.get_node(node["master"])
                if not target:
                    raise NodeNotFound(node["master"])
            else:
                masters = sorted(self.masters,
                                 key=lambda x: len(x.slaves(x.name)))
                target = masters[0]

            while not self.consistent():
                time.sleep(1)
            new.replicate(target.name)

    def fill_slots(self):
        masters = self.masters
        slots = itertools.chain(*[n.slots for n in masters])
        missing = list(set(range(CLUSTER_HASH_SLOTS)).difference(slots))

        div = divide_number(len(missing), len(masters))
        masters.sort(key=lambda x: len(x.slots))

        i = 0
        for count, node in zip(div, masters):
            node.addslots(*missing[i:count + i])
            i += count

    def migrate_node(self, src_node):
        nodes = [n for n in self.masters if n.name != src_node.name]
        slots = divide_number(len(src_node.slots), len(nodes))
        nodes.sort(key=lambda x: len(x.slots), reverse=True)
        for node, count in zip(nodes, slots):
            self.migrate(src_node, node, count)

    def migrate_slot(self, src, dst, slot, timeout=15000, verbose=True):
        dst.setslot("IMPORTING", slot, src.name)
        src.setslot("MIGRATING", slot, dst.name)
        for key in _scan_keys(src, slot):
            if verbose:
                echo("Migrating:", key)
            src.migrate(dst.host, dst.port, key, 0, timeout)

        for node in self.nodes:
            node.setslot("NODE", slot, dst.name)

    def migrate(self, src, dst, count):
        slots = src.slots
        slots_count = len(slots)
        if count > slots_count:
            count = slots_count

        keys = [(s, src.countkeysinslot(s)) for s in slots]
        keys.sort(key=lambda x: x[1])

        for slot, _ in keys[:count]:
            self.migrate_slot(src, dst, slot)


def slot_balance(seq, amt):
    avg = float(amt) / float(len(seq))
    seq.sort(key=lambda x: x["count"])

    lower, higher = map(int, (math.floor(avg), math.ceil(avg)))
    i, j = 0, len(seq) - 1
    while i < j:
        begin, end = seq[i], seq[j]

        need, more = lower - begin["count"], end["count"] - higher
        if not need:
            i += 1
        if not more:
            j -= 1

        if not need or not more:
            continue

        if need < more:
            begin["need"].append((end["node"], need))
            begin["count"] += need
            end["count"] -= need
            i += 1
        elif need > more:
            begin["need"].append((end["node"], more))
            begin["count"] += more
            end["count"] -= more
            j -= 1
        else:
            begin["need"].append((end["node"], need))
            begin["count"] += need
            end["count"] -= more
            i += 1
            j -= 1
    return seq
