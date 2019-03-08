# Grass

This tool targets a singel node job scheduling with resource constraints.

My use case is spawning machine learning tasks in a 4-GPU desktop with grid search and manage their results with [sacred]().

### Examples
```
grass create-queue exp1 --cpu=32 --gpu=2
grass enqueue exp1 --gpu=1 -- CUDA_VISIBLE_DEVICES={gpu} python train.py

grass create-queue exp2 --cpu=32 --gpu=[2,3]
for lr in 0.01 0.1
do
    grass enqueue exp2 --gpu=0.5 -- CUDA_VISIBLE_DEVICES={gpu} python train_exp2.py --lr=$lr
done

grass info
...
```

```py
import os

for lr in [0.01, 0.1]:
    for batch_size in [8, 16, 32]:
        os.system("grass enqueue exp1 --gpu=1 --cwd=/data/ -- CUDA_VISIBLE_DEVICES={gpu} python train.py")
```

### TODO
- [ ] In-memory queue (not persistent)
- [ ] ORM with sqlite/postgres backends
- [ ] Distributed
- [ ] A faster scheduler with O(log^k n) where $k$ is the number of resource types

### Goals
- General resource definition (not limited to cpu/memory)
- A local job sheduler (no cluster setup)
- Provide with arguments information about assigned resources.
- Fractional resource requirement (gpu=0.2)

### Alternatives
- Old Unix commands (wait, at, batch, nq) only support a single queue (no concurrency) or does not tell my process which queue (gpu) it is assigned.
- Distributed solutions like [Apache Airflow](), Slurm, or running each job in a container is overkill for my scenario.
- Light-weight distributed queues like celery may set max memory per worker, but are not aware of GPUs - at least you need to write a script to fetch the work from celery and spawn 4 processes for 4 gpus, each with CUDA_VISIBLE_DEVICES=index.
- FGLab is the closest, aware of CPU and GPU, able to generate a batch of hyperparameter configurations, but debugging is hard (failed job silently disappear) and aggregation is not as good as sacred. If you have train accuracies for a million iterations, you need to write them in json. For realtime metric, you need to update the json each iteration, which requires writing a million^2 numbers.
