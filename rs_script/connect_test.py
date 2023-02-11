import ray
import os
import numpy as np

os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"

@ray.remote
def foo():
    return True

ray.get(ray.remote())
