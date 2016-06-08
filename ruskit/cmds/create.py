import collections
import sys
import time
import uuid

from ruskit import cli
from ..cluster import ClusterNode, CLUSTER_HASH_SLOTS, Cluster, RuskitException
from ..utils import echo, spread, divide


def split_slot(n, m):
    chunks = divide(n, m)

    res, total = [], 0
    for c in chunks:
        res.append((total, c + total))
        total += c
    return res


class CreateClusterFail(RuskitException):
    pass


class NodeWrapper(object):
    def __init__(self, node):
        self.node = node

        self.origin = None
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

    def __repr__(self):
        return '{}:{}'.format(self.node.host, self.node.port)


class Manager(object):
    def __init__(self, slave_count, instances, master_count=0):
        assert slave_count >= 0

        self.slave_count = slave_count
        instances = [ClusterNode.from_uri(i) for i in instances]

        self.instances = [NodeWrapper(i) for i in instances]

        if not master_count:
            master_count = int(len(self.instances) / (slave_count + 1))
        self.master_count = master_count
        assert self.master_count >= 3, \
            "Redis Cluster requires at least 3 master nodes"

        self.masters = []
        self.slaves = []

    def check(self):
        versions = set()
        for instance in self.instances:
            info = instance.info()
            if not info.get("cluster_enabled"):
                raise CreateClusterFail("cluster not enabled")
            if info.get("db0"):
                raise CreateClusterFail("data exists in db0")
            if instance.cluster_info()["cluster_known_nodes"] != 1:
                raise CreateClusterFail(
                    "node {}:{} belong to other cluster".format(
                        instance.host, instance.port))
            versions.add(info['redis_version'])
        if len(versions) != 1:
            raise CreateClusterFail(
                "multiple versions found: {}".format(list(versions)))
        return True  # keep this for compability

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

        if self.slave_count > 0:
            plan = self.distribute_slaves(masters, slaves)
            for m, s in plan:
                if s.origin:
                    s.host = s.origin
                s.unassigned_master = m.name

        self.instances = [i for i in self.instances if i.active()]
        self.cluster = Cluster(self.instances)

    def distribute_slaves(self, masters, slaves):
        tracks = []

        def _tracked(master, slave, p):
            if not p:
                return False
            target = p[:]
            target.append((master, slave))
            return target in tracks

        while True:
            plan = []
            current_slaves = slaves[:]
            for master in masters:
                node = None
                if not current_slaves:
                    break

                for s in current_slaves:
                    if s.host == master.host or _tracked(master, s, plan):
                        continue
                    node = s
                    plan.append((master, s))
                    current_slaves.remove(s)
                    break
                if node is None:
                    nodes = [s for s in current_slaves
                             if s.host == master.host]
                    nodes[0].origin = nodes[0].host
                    nodes[0].host += str(uuid.uuid4())[:6]

            if len(plan) == min(len(slaves), len(masters)):
                break
            if plan:
                tracks.append(plan)

        more = set(slaves).difference([s for _, s in plan])

        if more:
            plan.extend(self.distribute_slaves(masters, list(more)))
        return plan

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
    try:
        manager.check()
    except CreateClusterFail as e:
        echo("Cluster can not be created: {}".format(e.message),
            color="red")
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
