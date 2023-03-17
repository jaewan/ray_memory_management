import ray
import numpy as np

@ray.remote
def producer():
    return np.zeros(400_000_000)

@ray.remote
def consumer(obj1, obj2):
    return True

ray.init()

obj1 = producer.remote()
obj2 = producer.remote()
obj3 = producer.remote()
obj4 = producer.remote()

res = []
res.append(consumer.remote(obj1, obj3))
res.append(consumer.remote(obj2, obj4))

for r in res:
    ray.get(r)
