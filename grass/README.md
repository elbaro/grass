# Grass

[![Build Status](https://travis-ci.org/elbaro/grass.svg)](https://travis-ci.org/elbaro/grass) [![Code Coverage](https://codecov.io/gh/elbaro/grass/branch/master/graph/badge.svg)](https://codecov.io/gh/elbaro/grass)

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
grass daemon
grass create-queue job1 --capacity "{gpu:4,cpu:16}" main.py arg1
grass enqueue --require "{gpu:1}" --env "CUDA_VISIBLE_DEVICES={gpu}" job1 arg2 arg3
grass enqueue --require "{gpu:0.5}" --env "CUDA_VISIBLE_DEVICES={gpu}" job2 arg2 arg3
grass enqueue  python train.py
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
Create a self-signed cert
```
openssl req -x509 -newkey rsa:4096 -keyout myKey.pem -out cert.pem -days 365 -nodes
openssl pkcs12 -export -out keyStore.p12 -inkey myKey.pem -in cert.pem

```

Broker
```
grass daemon --bind 0.0.0.0:7500 --no-worker --cert my_cert.p12
```

Worker
```
grass daemon --no-broker --connect my.server.com:7500
grass create-queue --capacity "{gpu:4}" cifar100 python train.py
grass create-queue --capacity "{printer:1}" print lpr
```

CLI (from anywhere)
```
grass enqueue --broker 1.2.3.4:7500 --require "{printer:1}" print paper.pdf
grass enqueue --broker 1.2.3.4:7500 --require "{gpu:1}" cifar100 --lr=0.01
grass show --broker 1.2.3.4:7500
```

## TODO
- [x] In-memory queue (not persistent)
- [x] Going distributed without complex cluster setup
- [x] Supports a worker behind firewall
- [x] Fractional resource requirement (gpu=0.2)
- [ ] DNS resolve
- [ ] Request / Allocate multiple devices (e.g. gpu:2.5)
- [ ] Persistency with sqlite/postgres backends
- [ ] Journaling with backends. Recover from failure.
- [ ] A faster scheduler with O(log^k n) where $k$ is the number of resource types
