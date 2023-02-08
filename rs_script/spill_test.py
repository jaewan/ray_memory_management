import ray
import numpy as np

@ray.remote
def huge():
    x = np.zeros((2 ** 30,))
    x[0] = 1

