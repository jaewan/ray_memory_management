import ray
import numpy as np
import os

os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"

@ray.remote
def huge():
    x = np.zeros((10 ** 9, 2))
    return x

ray.get(huge.remote())
