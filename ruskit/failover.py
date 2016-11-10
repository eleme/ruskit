from copy import copy
import logging
from itertools import izip_longest

import redis

from .cluster import Cluster, ClusterNode
from .distribute import gen_distribution, NodeWrapper


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class FastAddMachineManager(object):
    def __init__(self, cluster, new_nodes):
        self.cluster = cluster
        self.new_nodes = new_nodes
        self.result = None
        self.dis = gen_distribution(self.cluster.nodes, new_nodes)

    def get_distribution(self):
        hosts = map(copy, self.dis['hosts'])
        masters = self.dis['masters']
        slaves = self.dis['slaves']
        result = self.peek_result()
        plan = result['plan']
        deleted_masters = [p['master'] for p in plan]
        new_slaves = []
        new_masters = []
        for p in plan:
            m = copy(p['master'])
            s = copy(p['slave'])
            # After failover, their roles will change
            m.slaves = []
            m.master = s
            s.slaves.append(m)
            s.master = None
            new_masters.append(s)
            new_slaves.append(m)
        new_slaves = NodeWrapper.divide_by_host(new_slaves, len(hosts))
        new_masters = NodeWrapper.divide_by_host(new_masters, len(hosts))

        masters = [list(set(m) - set(deleted_masters)) for m in masters]
        masters = [o + n for o, n in zip(masters, new_masters)]
        slaves = [o + n for o, n in zip(slaves, new_slaves)]
        return {
            'hosts': hosts,
            'masters': masters,
            'slaves': slaves,
            'frees': NodeWrapper.divide_by_host([], len(hosts)),
        }

    def peek_result(self):
        if self.result:
            return self.result
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
        masters = map(copy, self.dis['masters'])
        frees = filter(None, map(copy, self.dis['frees']))
        hosts_len = len(self.dis['hosts'])
        masters_sum = len(sum(masters, []))
        aver = (masters_sum + hosts_len - 1) / hosts_len
        new_hosts_num = len(set(n.host_index for n in sum(frees, [])))
        num_in_old_hosts = masters_sum / \
            hosts_len * (hosts_len - new_hosts_num) + masters_sum % hosts_len
        moved_masters = masters_sum - num_in_old_hosts
        # merge [[a,b], [c], [d,e,f]] to [a,c,d,b,e,f] for example
        frees = filter(None, sum(map(list, izip_longest(*frees)), []))
        while moved_masters > 0 and len(frees) > 0:
            m = self.select_master(masters)
            f = frees.pop(0)
            f.master = m
            m.slaves.append(f)
            plan.append({
                'slave': f,
                'master': m,
            })
            moved_masters -= 1
        self.result = {
            'plan': plan,
            'frees': frees,
        }
        return self.result

    def select_master(self, masters):
        nums = map(len, masters)
        m = max(nums)
        i = nums.index(m)
        return masters[i].pop()
