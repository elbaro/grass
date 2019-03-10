# Grass

Grass is a non-distributed version of slurm.

My use case is spawning machine learning tasks in a 4-GPU desktop with grid search or bayes optimization, and manage their results with [sacred](https://github.com/IDSIA/sacred).

### Status
Very early stage, but it is working. The scheduler is FIFO, does not account for fairness or efficiency. If you have homogeneous jobs, it can serve well.

Yet there is no plugin to handle cpu or gpu as in slurm.

### Examples
```
grass create-queue q1 --json "{gpu:4,cpu:16}"
grass enqueue q1 --env "CUDA_VISIBLE_DEVICES={gpu}" --json "{gpu:0.5}" -- python train.py lr=0.01
```
q1 has gpu 0, 1, 2, 3. We enqueued a job that requests 0.5 gpu. {gpu} is replaced by one of 0,1,2,3.
Note: curly braces should be wrapped since they are special chars in bash/zsh.



Python script to generate jobs:
```py
import os

for lr in [0.01, 0.1]:
    for batch_size in [8, 16, 32]:
        os.system("grass enqueue q1 --json '{gpu:0.5}' --cwd=/data/ --env='CUDA_VISIBLE_DEVICES={gpu}' -- CUDA_VISIBLE_DEVICES={gpu} python train.py lr=%s batch_size=%s" % (lr,batch_size))
```

### Usage
1. Start a daemon in background
`grass start`

2. Create a queue
```grass create-queue q1 --json '{gpu:2,cpu:16}'```
You can specify resources with `--json` or `--file`. Grass uses json5, which allows you to omit quotes.
Currently only `--json` is supported.

3. Enqueue jobs
```grass enqueue q1 --json '{gpu:0.5}' python train.py```
You can also specify `--cwd` or `--env`.
```grass enqueue q1 --env PYTHONPATH=.. --env TERM=.. --cwd /data --json '{gpu:0.5}' python train.py```

4. Inspect jobs
```grass show```
```grass show q1```

### JSON Syntax
When you create a queue, you can specify resource capacity in three ways.
1. a single integer
`{gpu:4}`
This means you have 4 gpus, each with capacity 1. Equivalent to `gpu:{'0':1,'1':1,'2':1,'3':1}`.

2. A list of floats
`{hdd:[32,64,128]}`
This means you have 3 hdds, with capacity 32, 64, 128. Equivalent to `hdd:{'0':32,'1':64,'2':128}`.

3. A dict of string:float
`{gpu:{'2':1,'4':1}}`
This means you have 2 gpus, with names '2' and '4'. {gpu} in your command line will be replaced with '2' and '4'. Useful if your resource is not 0-indexed or you want only a subset of resource.

When you enqueue a job, it is always `{resource_type1: amount1, resource_type2: amount2, ..}`.

If you write `{RAM:512}`, this means you have RAM0~RAM511 each with capacity 1. You probably want `{RAM:[512]}`, which represents RAM0 with capacity 512.

### TODO
- [x] In-memory queue (not persistent)
- [ ] Request / Allocate multiple devices (e.g. gpu:2.5)
- [ ] Persistency with sqlite/postgres backends
- [ ] Journaling with backends. Recover from failure.
- [ ] Going distributed without complex cluster setup
- [ ] A faster scheduler with O(log^k n) where $k$ is the number of resource types

### Goals
- General resource definition (not limited to cpu/memory)
- Jobs are aware which resources are allocated to them.
- Fractional resource requirement (gpu=0.2)

### Alternatives
- Old Unix commands (wait, at, batch, nq) only support a single queue (no concurrency) or does not tell my process which queue (gpu) it is assigned.
- Distributed solutions like [Apache Airflow](), Slurm, or running each job in a container is overkill for my scenario.
- Light-weight distributed queues like celery may set max memory per worker, but are not aware of GPUs - at least you need to write a script to fetch the work from celery and spawn 4 processes for 4 gpus, each with CUDA_VISIBLE_DEVICES=index.
- FGLab is the closest, aware of CPU and GPU, able to generate a batch of hyperparameter configurations, but debugging is hard (failed job silently disappear) and aggregation is not as good as sacred. If you have train accuracies for a million iterations, you need to write them in json. For realtime metric, you need to update the json each iteration, which requires writing a million^2 numbers. A similar project sacred has a good logging system, but has no scheduling feature.
