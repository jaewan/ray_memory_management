import ray
import numpy as np
import os
import time
from sys import getsizeof

# os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"

@ray.remote
def huge(bool):
    if bool:
        x = np.zeros(14 * 6 ** 9)
        print(getsizeof(x))
        return x
    else:
        y = np.zeros(14 * 6 ** 9)
        print(getsizeof(y))
        return y

@ray.remote
def add(numpy1, numpy2):
    return numpy1 + numpy2

future1 = huge.remote(True)
future2 = huge.remote(False)
result1 = ray.get(future1)
print(f"result 1 passed")
del future1 
del result1
time.sleep(5)
result2 = ray.get(future2)
print(f"result 2 passed")
print(result2)

