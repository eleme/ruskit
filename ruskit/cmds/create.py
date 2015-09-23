import collections
import time
import sys

from ruskit import cli
from ..cluster import ClusterNode, CLUSTER_HASH_SLOTS, Cluster
from ..utils import echo, spread, divide


def split_slot(n, m):
    chunks = divide(n, m)

    res, total = [], 0
    for c in chunks:
        res.append((total, c + total))
        total += c
    return res


class NodeWrapper(object):
    def __init__(self, node):
        self.node = node

        self.unassigned_slots = []
        self.unassigned_master = None

    def __getattr__(self, attr):
        return getattr(self.node, attr)

    def assign_master(self):
        assert self.unassigned_master
        self.replicate(self.unassigned_master)
        self.unassigned_master = None

    def assign_slots(self):
        assert self.unassigned_slots
        for chunk in self.unassigned_slots:
            self.addslots(*range(*chunk))

    def active(self):
        return bool(self.unassigned_slots or self.unassigned_master)


class Manager(object):
    def __init__(self, slave_count, instances, master_count=0):
        assert slave_count >= 0

        self.slave_count = slave_count
        instances = [ClusterNode.from_uri(i) for i in instances]

        self.instances = [NodeWrapper(i) for i in instances]

        if not master_count:
            master_count = len(self.instances) / (slave_count + 1)
        self.master_count = master_count
        assert self.master_count >= 3, \
            "Redis Cluster requires at least 3 master nodes"

        self.masters = []
        self.slaves = []

    def check(self):
        for instance in self.instances:
            info = instance.info()
            if not info.get("cluster_enabled") or info.get("db0") or \
                    instance.cluster_info()["cluster_known_nodes"] != 1:
                return False
        return True

    def init_slots(self):
        ips = collections.defaultdict(list)
        for instance in self.instances:
            ips[instance.host].append(instance)

        self.masters = masters = spread(ips, self.master_count)

        chunks = split_slot(CLUSTER_HASH_SLOTS, self.master_count)
        for master, chunk in zip(masters, chunks):
            master.unassigned_slots.append(chunk)

        slaves = spread(ips, sum(len(i) for i in ips.values()))

        self.slaves = slaves[:] if self.slave_count > 0 else []

        while slaves and self.slave_count > 0:
            self.distribute_slaves(masters, slaves)

        self.instances = [i for i in self.instances if i.active()]
        self.cluster = Cluster(self.instances)

    def distribute_slaves(self, masters, slaves):
        for master in masters:
            assigned_slaves = 0
            while assigned_slaves < self.slave_count and slaves:
                node = None
                for slave in slaves:
                    if slave.host != master.host:
                        node = slave
                        break
                if not node:
                    node = slaves.pop(0)
                else:
                    slaves.remove(node)
                node.unassigned_master = master.name
                assigned_slaves += 1

    def show_cluster_info(self):
        for instance in self.instances:
            if instance.unassigned_master:
                echo('S', end='', color="yellow")
            else:
                echo('M', end='', color="green")
            name_msg = ": {name} {host}:{port}"
            echo(name_msg.format(name=instance.name,
                                 host=instance.host, port=instance.port))
            if instance.unassigned_master:
                echo("   replicates:", instance.unassigned_master)
            else:
                slot_msg = ','.join(['-'.join([str(s[0]), str(s[1] - 1)])
                                     for s in instance.unassigned_slots])
                echo("   slots:", slot_msg)

    def set_slots(self):
        for master in self.masters:
            master.assign_slots()

    def set_slave(self):
        for slave in self.slaves:
            slave.assign_master()

    def join_cluster(self):
        if not self.instances:
            return

        first_instance = self.instances[0]
        for instance in self.instances[1:]:
            instance.meet(first_instance.host, first_instance.port)

    def assign_config_epoch(self):
        epoch = 1
        for instance in self.instances:
            try:
                instance.set_config_epoch(epoch)
            except:
                pass
            epoch += 1


@cli.command
@cli.argument("-s", "--slaves", type=int, default=0)
@cli.argument("-m", "--masters", type=int, default=0)
@cli.argument("instances", nargs='+')
def create(args):
    manager = Manager(args.slaves, args.instances, args.masters)
    if not manager.check():
        echo("Cluster can not be created.\n", color="red")
        echo("To be a cluster member:")
        echo("    1. enable cluster mode")
        echo("    2. not a member of other clusters")
        echo("    3. no data in db 0")
        exit()
    manager.init_slots()
    manager.show_cluster_info()

    manager.set_slots()
    manager.assign_config_epoch()
    manager.join_cluster()

    echo("Waiting for the cluster to join ", end='')
    sys.stdout.flush()
    time.sleep(1)
    while not manager.cluster.consistent():
        echo('.', end='')
        sys.stdout.flush()
        time.sleep(1)
    echo()
    manager.set_slave()

    if manager.cluster.consistent() or manager.cluster.all_slots_covered():
        echo("Done.", color="green")
    else:
        echo("Failed.", color="red")
