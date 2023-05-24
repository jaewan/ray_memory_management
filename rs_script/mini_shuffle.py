# import ray
# import numpy as np
# import os
# import time
# from sys import getsizeof

# # os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"

# @ray.remote(num_cpus=1)
# class HeadActor(object):
#     def __init__(self):
#         pass

#     def producer(self):
#         x = np.zeros(700_000_000//8)
#         print(getsizeof(x))
#         return x
    
#     def consumer(self, obj1, obj2):
#         return True

# headActor = HeadActor.remote()
# obj1 = headActor.producer.remote()
# obj2 = headActor.producer.remote()

# # res1 = ray.get(obj1)
# # print("Object 1 created")

# res1 = headActor.consumer.remote(obj1, obj2)

# ray.get(res1)

# # res2 = headActor.consumer.remote(obj1, obj2)


# # del obj2 
# # del res2
# # time.sleep(5)

# # res1 = headActor.consumer.remote(obj1)
# # print("Object 1 pulled!")


import ray
import time
import numpy as np

OBJ_STORE_SIZE = 3_000_000_000

@ray.remote
def producer():
 return np.zeros(OBJ_STORE_SIZE//8)

@ray.remote
def consumer(obj):
 return True

a = producer.remote()
time.sleep(3)
b = producer.remote()
ray.get(b)

print("Spilled to remote node")

res = consumer.remote(a)
# ray a
print(ray.get(res))

res1 = consumer.remote(b)
print(ray.get(res1))

print("Success")