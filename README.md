# Grass

[![Build Status](https://travis-ci.org/elbaro/grass.svg)](https://travis-ci.org/elbaro/grass) [![Code Coverage](https://codecov.io/gh/elbaro/grass/branch/master/graph/badge.svg)](https://codecov.io/gh/TechnionYP5777/project-name)

[Wiki](https://github.com/elbaro/grass/wiki) | [Releases](https://github.com/elbaro/grass/releases) | [Changelog](https://github.com/elbaro/grass/blob/master/CHANGELOG.md)

Grass is a non-distributed version of slurm.

Supports and tested on Unix.

## Install
requires Rust nightly.
```
cargo install --git https://github.com/elbaro/grass
```

## Example - Local setup
```
grass daemon --resources "{gpu:4,cpu:16}"
```
```
grass enqueue --require "{gpu:1}" --env "CUDA_VISIBLE_DEVICES={gpu}" python train.py
```

grass supports json5.

Python script to generate jobs:
```py
import os

for lr in [0.01, 0.1]:
    for batch_size in [8, 16, 32]:
        os.system("grass enqueue --require '{gpu:0.5}' --cwd=/data/ --env='CUDA_VISIBLE_DEVICES={gpu}' -- python train.py lr=%s batch_size=%s" % (lr,batch_size))
```

## Example - Multi nodes

Broker
```
grass daemon --bind 0.0.0.0:7500 --no-worker
```

Worker
```
grass daemon --no-broker --connect my.server.com:7500
```

CLI (from anywhere)
```
grass enqueue --broker my.server.com:7500 echo 123
grass enqueue --broker my.server.com:7500 sleep 10
grass show
```

A worker behind firewall works. It does not use additional TCP connectcion.

## TODO
- [x] In-memory queue (not persistent)
- [x] Going distributed without complex cluster setup
- [x] Supports a worker behind firewall
- [ ] Fractional resource requirement (gpu=0.2)
- [ ] Request / Allocate multiple devices (e.g. gpu:2.5)
- [ ] Persistency with sqlite/postgres backends
- [ ] Journaling with backends. Recover from failure.
- [ ] A faster scheduler with O(log^k n) where $k$ is the number of resource types
