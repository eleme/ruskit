import argparse

from cluster import ClusterNode


def destroy_cluster(instances):
    instances = [ClusterNode(*(i.split(':'))) for i in instances]

    for instance in instances:
        if instance.is_slave():
            instance.failover(takeover=True)

        if instance.slots:
            instance.delslots(*instance.slots)

        if len(instance.nodes()) <= 1:
            continue

        remotes = instance.nodes()[1:]
        for remote in remotes:
            print("Forgeting: {}:{} -> {}".format(
                instance.host, instance.port, remote["addr"]))
            instance.forget(remote["name"])


def main():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument("instances", nargs='+')
    args = parser.parse_args()

    destroy_cluster(args.instances)


if __name__ == '__main__':
    main()
