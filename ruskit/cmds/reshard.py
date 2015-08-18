import argparse

from cluster import Cluster


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cluster")

    args = parser.parse_args()

    cluster = Cluster(args.cluster)
    cluster.reshard()


if __name__ == "__main__":
    main()
