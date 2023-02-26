import ray
import numpy as np
import os
import time
from sys import getsizeof

# os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"

@ray.remote(num_cpus=1)
class HeadActor(object):
    def __init__(self):
        pass

    def huge(self, bool):
        if bool:
            x = np.zeros(700_000_000//8)
            print(getsizeof(x))
            return x
        else:
            y = np.zeros(700_000_000//8)
            print(getsizeof(y))
            return y

# def huge(bool):
#     if bool:
#         x = np.zeros(700_000_000//8)
#         print(getsizeof(x))
#         return x
#     else:
#         y = np.zeros(700_000_000//8)
#         print(getsizeof(y))
#         return y

@ray.remote
def add(numpy1, numpy2):
    return numpy1 + numpy2

headActor = HeadActor.remote()
future1 = headActor.huge.remote(True)
time.sleep(3)
result1 = ray.get(future1)
print(f"obj 1 created")
del result1
future2 = headActor.huge.remote(False)
result2 = ray.get(future2)
print(f"Obj1 spilled, obj2 created")
del future2 
del result2
time.sleep(5)
result1 = ray.get(future1)
print(f"object1 pulled!")

time.sleep(5)
del future1
del result1
future1 = headActor.huge.remote(True)
time.sleep(3)
result1 = ray.get(future1)
print(f"obj 1 created again")
del result1
future2 = headActor.huge.remote(False)
result2 = ray.get(future2)
print(f"Obj1 spilled, obj2 created again")
del future2 
del result2
time.sleep(5)
result1 = ray.get(future1)
print(f"object1 pulled again!")
