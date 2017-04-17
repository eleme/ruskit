# -*- coding: utf-8 -*-

import datetime
import redis
import pprint

from ruskit import cli
from ..cluster import Cluster, ClusterNode
from ..utils import echo
from ..distribute import print_cluster, gen_distribution
from ..utils import timeout_argument
from ..health import HealthCheckManager


@cli.command
@cli.argument("cluster")
@timeout_argument
def info(args):
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    dis = []
    for n in cluster.masters:
        slaves = ','.join([s["addr"] for s in n.slaves(n.name)])
        msg = "{} {}:{} {} {}".format(n.name, n.host, n.port, len(n.slots),
                                      slaves)
        dis.append(msg)
    echo("\n".join(dis))
    echo("Masters:", len(cluster.masters))
    echo("Instances:", len(cluster.nodes))
    echo("Slots:", sum(len(n.slots) for n in cluster.masters))

    random_node = cluster.nodes[0]
    connection_pool = random_node.r.connection_pool
    connection = connection_pool._available_connections[0]
    echo('Exception Classes:')
    pprint.pprint(connection._parser.EXCEPTION_CLASSES)


@cli.command
@cli.argument("cluster")
@timeout_argument
def slowlog(args):
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    slow_logs = cluster.get_slow_logs()
    for master, logs in slow_logs.iteritems():
        echo("Node: ", "%s:%s" % (master.host, master.port))
        for log in logs:
            time = datetime.datetime.fromtimestamp(log['start_time'])
            echo(
                "\t",
                time,
                "%s%s" % (log['duration'], "μs"),
                repr(log['command'])
                )


@cli.command
@cli.argument("cluster")
@timeout_argument
def fix(args):
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    cluster.fix_open_slots()
    cluster.fill_slots()


@cli.command
@cli.argument("cluster")
@cli.argument("nodes", nargs='+')
@timeout_argument
def delete(args):
    """Delete nodes from the cluster
    """
    nodes = [ClusterNode.from_uri(n) for n in args.nodes]
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))

    echo("Deleting...")
    for node in nodes:
        cluster.delete_node(node)
        cluster.wait()


@cli.command
@cli.argument("src")
@cli.argument("-d", "--dst")
@cli.argument("-s", "--slot", type=int)
@cli.argument("-c", "--count", type=int)
@cli.argument("-i", "--income", action="store_true")
@timeout_argument
@cli.pass_ctx
def migrate(ctx, args):
    src = ClusterNode.from_uri(args.src)
    cluster = Cluster.from_node(src)

    if args.dst:
        dst = ClusterNode.from_uri(args.dst)

    if args.dst and args.slot is not None:
        try:
            cluster.migrate_slot(src, dst, args.slot, verbose=True)
        except redis.ResponseError as e:
            ctx.abort(str(e))
    elif args.dst:
        count = len(src.slots) if args.count is None else args.count
        cluster.migrate(src, dst, count)
    else:
        cluster.migrate_node(src, args.count, income=args.income)

    cluster.wait()


@cli.command
@cli.argument("cluster")
@timeout_argument
def reshard(args):
    """Balance slots in the cluster.

    This command will try its best to distribute slots equally.
    """
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    cluster.reshard()


@cli.command
@cli.argument("node")
@cli.argument("master")
@timeout_argument
@cli.pass_ctx
def replicate(ctx, args):
    """Make node to be the slave of a master.
    """
    slave = ClusterNode.from_uri(args.node)
    master = ClusterNode.from_uri(args.master)
    if not master.is_master():
        ctx.abort("Node {!r} is not a master.".format(args.master))

    try:
        slave.replicate(master.name)
    except redis.ResponseError as e:
        ctx.abort(str(e))

    Cluster.from_node(master).wait()


@cli.command
@cli.argument("cluster")
@timeout_argument
def destroy(args):
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    for node in cluster.masters:
        node.flushall()

    for node in cluster.nodes:
        node.reset(hard=True)


@cli.command
@cli.argument("cluster")
@timeout_argument
def flushall(args):
    """Execute flushall in all cluster nodes.
    """
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    for node in cluster.masters:
        node.flushall()


@cli.command
@cli.argument("cluster")
@cli.argument("name")
@cli.argument("value")
@cli.argument("--config-command", default="config")
@cli.argument("--rewrite", action="store_true")
@timeout_argument
@cli.pass_ctx
def reconfigure(ctx, args):
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    if not cluster:
        ctx.abort("Cluster not exists")

    for node in cluster.nodes:
        echo("Setting `%s` of `%s` to `%s`" % (args.name, node, args.value))
        node.execute_command(args.config_command + " SET",
                             args.name, args.value)
        if args.rewrite:
            node.execute_command(args.config_command + " REWRITE")


@cli.command
@cli.argument("cluster")
@cli.argument("command", nargs='+')
@timeout_argument
@cli.pass_ctx
def cmd(ctx, args):
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    command = args.command
    for n in cluster.nodes:
        print n.execute_command(*command)


@cli.command
@cli.argument("cluster")
@timeout_argument
@cli.pass_ctx
def peek(ctx, args):
    cluster = Cluster.from_node(ClusterNode.from_uri(args.cluster))
    if not cluster.consistent():
        ctx.abort("Cluster not consistent.")
    dist = gen_distribution(cluster.nodes, [])
    print_cluster(dist)


@cli.command
@cli.argument("nodes", nargs='+')
@timeout_argument
@cli.pass_ctx
def check(ctx, args):
    nodes = [ClusterNode.from_uri(n) for n in args.nodes]
    report = HealthCheckManager(nodes).check()

    if report is None:
        print 'cluster is healthy'
        return

    for check_name, diff in report.iteritems():
        print '#' * 30
        print check_name
        print '#' * 30
        pprint.pprint(diff)
