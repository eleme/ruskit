import logging
from itertools import izip_longest

import redis

from .cluster import Cluster, ClusterNode
from .distribute import gen_distribution


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class FastAddMachineManager(object):
    def __init__(self, cluster, new_nodes):
        self.cluster = cluster
        self.new_nodes = new_nodes
        self.result = None

    def get_distribution(self):
        return gen_distribution(self.cluster.nodes, self.new_nodes)

    def peek_result(self):
        if self.result:
            return result
        return self.gen_plan(self.new_nodes)

    def move_masters_to_new_hosts(self, fast_mode=False):
        result = self.gen_plan(self.new_nodes)
        plan = result['plan']
        logger.info('move plan: {}'.format(plan))
        self.add_tmp_slaves(plan, fast_mode)
        self.promote_new_masters(plan)

    def add_tmp_slaves(self, plan, fast_mode):
        nodes = []
        for p in plan:
            nodes.append({
                'cluster_node': p['slave'].node,
                'role': 'slave',
                'master': p['master'].name,
            })
        self.cluster.add_slaves(nodes, fast_mode)
        self.cluster.wait()

    def promote_new_masters(self, plan):
        retry = 5
        new_masters = [p['slave'].node for p in plan]
        while len(new_masters) and retry > 0:
            for m in new_masters:
                try:
                    m.failover(takeover=True)
                except redis.RedisError as e:
                    logger.error(str(e))
            self.cluster.wait()
            for n in new_masters:
                n.flush_cache()
            new_masters = [n for n in new_masters if not n.is_master()]
            if len(new_masters) == 0:
                break
            retry -= 1
            logger.warning(
                'The nodes below are still slaves: {}'.format(new_masters))

    def gen_plan(self, new_nodes):
        plan = []
        dis = gen_distribution(self.cluster.nodes, new_nodes)
        masters = dis['masters']
        frees = filter(None, dis['frees'])
        hosts_len = len(dis['hosts'])
        masters_sum = len(sum(masters, []))
        aver = (masters_sum + hosts_len - 1) / hosts_len
        failover_masters = sum([h[aver:] for h in masters], [])
        # merge [[a,b], [c], [d,e,f]] to [a,c,e,b,e,f] for example
        frees = filter(None, sum(map(list, izip_longest(*frees)), []))
        while len(failover_masters) > 0 and len(frees) > 0:
            m = failover_masters.pop()
            f = frees.pop()
            plan.append({
                'slave': f,
                'master': m,
            })
        self.result = {
            'plan': plan,
            'frees': frees,
        }
        return self.result
