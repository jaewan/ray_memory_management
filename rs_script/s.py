import ray
import time
import numpy as np

def shuffle(object_size, npartitions, object_store_size):
  @ray.remote
  def map(npartitions, object_size):
    size = object_size//8
    data = np.random.rand(size)
    size = size//npartitions
    time.sleep(1)
    return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))
  @ray.remote
  def reduce(*partitions):
    #time.sleep(1)
    return True
  shuffle_start = time.perf_counter()
  # npartitions = (OBJECT_STORE_SIZE*WORKING_SET_RATIO)//OBJECT_SIZE
  refs = [map.options(num_returns=npartitions).remote(npartitions, object_size)
      for _ in range(npartitions)]
  results = []
  for j in range(npartitions):
    results.append(reduce.remote(*[ref[j] for ref in refs]))
  del refs
  ray.get(results)
  del results
  shuffle_end = time.perf_counter()
  return shuffle_end - shuffle_start

def main(args):
    shuffle(args.object_size, args.npartitions, args.object_store_size)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--object-size", required=True, type=int)
    parser.add_argument("--npartitions", required=True, type=int)
    parser.add_argument("--object-store-size", type=int)
    inputs = parser.parse_args()
    main(inputs)