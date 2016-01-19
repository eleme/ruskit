# -*- coding: utf-8 -*-

from redis.exceptions import (
    ResponseError, RedisError,
)


class ClusterError(RedisError):
    pass


class ClusterDownError(ClusterError, ResponseError):
    def __init__(self, resp):
        self.args = (resp,)
        self.message = resp


class AskError(ResponseError):
    """
    partially keys is slot migrated to another node

    src node: MIGRATING to dst node
        get > ASK error
        ask dst node > ASKING command
    dst node: IMPORTING from src node
        asking command only affects next command
        any op will be allowed after asking command
    """

    def __init__(self, resp):
        """should only redirect to master node"""
        self.args = (resp,)
        self.message = resp
        slot_id, new_node = resp.split(' ')
        host, port = new_node.rsplit(':', 1)
        self.slot_id = int(slot_id)
        self.node_addr = self.host, self.port = host, int(port)


class MovedError(AskError):
    """
    all keys in slot migrated to another node
    """
    pass


class UnknownNodeError(ResponseError):
    """
    related node is no longer available on the cluster
    """

    def __init__(self, resp):
        self.node_id = resp

    def __unicode__(self):
        return self.node_id
