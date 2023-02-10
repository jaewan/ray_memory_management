import ray
import numpy as np

@ray.remote
def huge():
    x = np.zeros((10 ** 9, 2))
    return x

ray.get(huge.remote())
