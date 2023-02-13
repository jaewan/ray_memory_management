import ray
import numpy as np
import os
from sys import getsizeof

os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"

@ray.remote
def huge():
    x = np.zeros((10 ** 9, 2))
    print(getsizeof(x))
    return x

ray.get(huge.remote())
