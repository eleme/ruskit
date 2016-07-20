from __future__ import print_function

import itertools
import os
import sys

COLOR_MAP = {
    "red": 31,
    "green": 32,
    "yellow": 33,
    "blue": 34,
    "purple": 35
}


def echo(*values, **kwargs):
    end = kwargs.get("end", '\n')
    color = kwargs.get("color", None)
    bold = 0 if kwargs.get("bold", False) is False else 1
    disable = kwargs.get("diable", False)

    if disable:
        return

    msg = ' '.join(str(v) for v in values) + end

    if not color or os.getenv("ANSI_COLORS_DISABLED") is not None:
        sys.stdout.write(msg)
    else:
        color_prefix = "\033[{};{}m".format(bold, COLOR_MAP[color])
        color_suffix = "\033[0m"
        sys.stdout.write(color_prefix + msg + color_suffix)
    sys.stdout.flush()


def divide(n, m):
    """Divide integer n to m chunks
    """
    avg = int(n / m)
    remain = n - m * avg
    data = list(itertools.repeat(avg, m))
    for i in range(len(data)):
        if not remain:
            break
        data[i] += 1
        remain -= 1
    return data


def spread(nodes, n):
    """Distrubute master instances in different nodes

    {
        "192.168.0.1": [node1, node2],
        "192.168.0.2": [node3, node4],
        "192.168.0.3": [node5, node6]
    } => [node1, node3, node5]
    """
    target = []

    while len(target) < n and nodes:
        for ip, node_group in list(nodes.items()):
            if not node_group:
                nodes.pop(ip)
                continue
            target.append(node_group.pop(0))
            if len(target) >= n:
                break
    return target


class RuskitException(Exception):
    pass


class InvalidNewNode(RuskitException):
    pass


def check_new_nodes(new_nodes, old_nodes=None):
    if old_nodes is None:
        old_nodes = []

    versions = set(n.info()['redis_version'] for n in old_nodes)
    for instance in new_nodes:
        info = instance.info()
        if not info.get("cluster_enabled"):
            raise InvalidNewNode("cluster not enabled")
        if info.get("db0"):
            raise InvalidNewNode("data exists in db0 of {}".format(instance))
        if instance.cluster_info()["cluster_known_nodes"] != 1:
            raise InvalidNewNode(
                "node {}:{} belong to other cluster".format(
                    instance.host, instance.port))
        versions.add(info['redis_version'])
    if len(versions) != 1:
        raise InvalidNewNode(
            "multiple versions found: {}".format(list(versions)))
