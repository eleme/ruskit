# Ruskit

[![Build Status](https://travis-ci.org/eleme/ruskit.svg?branch=master)](https://travis-ci.org/eleme/ruskit)
[![Version](https://img.shields.io/pypi/v/ruskit.svg)](https://pypi.python.org/pypi/ruskit)

Redis cluster administration toolkit.

## Usage

```bash
pip install ruskit
# Note that `addslaves` command requires python-igraph which is not installed by default
# Run the command below if you need `addslaves`
pip install ruskit[addslaves]
```

##### Create cluster

```bash
ruskit create -s 1 192.168.0.11:{8000,8001,8002} 192.168.0.12:{8000,8001,8002}
```

##### Add nodes

```bash
# Add masters:
ruskit add 192.168.0.11:8000 192.168.0.13:8000 192.168.0.14:8000

# Add slaves:
# ruskit add <node belong to cluster> <slave node>,<master node>
ruskit add 192.168.0.11:8000 192.168.0.14:8001,192.168.0.13:8000

# ruskit addslaves [--peek] [--slaves-limit <max slaves count>] <node belong to cluster> <slave node> [<slave node> ...]
ruskit addslaves --peek --slaves-limit 3 192.168.0.11:8000 192.168.0.14:8001 192.168.0.14:8002
```

##### Query cluster info

```bash
ruskit info 192.168.0.11:8000
```

##### Delete nodes

```bash
ruskit delete 192.168.0.11:8000 192.168.0.13:8000
```

##### Migrate slots

```bash
# migrate 100 slots from 192.168.0.11:8000 to 192.168.0.12:8000
ruskit migrate -d 192.168.0.12:8000 -c 100 192.168.0.11:8000

# migrate all slots from 192.168.0.11:8000 to 192.168.0.12:8000
ruskit migrate -d 192.168.0.12:8000 192.168.0.11:8000

# migrate slot 866 from 192.168.0.11:8000 to 192.168.0.12:8000
ruskit migrate -d 192.168.0.12:8000 -s 866 192.168.0.11:8000

# migrate 100 slots from 192.168.0.11:8000 to other nodes in the cluster
ruskit migrate -c 100 192.168.0.11:8000

# migrate 100 slots from the cluster to 192.168.0.11:8000
ruskit migrate -c 100 -i 192.168.0.11:8000
```

##### Balance slots

```bash
ruskit reshard 192.168.0.11:8000
```

##### Fix cluster

```bash
ruskit fix 192.168.0.11:8000
```

##### Replicate

```bash
ruskit replicate 192.168.0.14:8001 192.168.0.11:8000
```

##### Destroy cluster

```bash
ruskit destroy 192.168.0.11:8000
```

##### Flushall data

```bash
ruskit flushall 192.168.0.11:8000
```

##### View slowlog(Recent 128 logs per node)

```bash
ruskit slowlog 192.168.0.11:8000
```

##### View nodes distribution
```bash
# ruskit peek <node belong to cluster>
ruskit peek 192.168.0.11:8000
```

## Test
```
pip install -r test_requirements.txt
py.test tests/
```
