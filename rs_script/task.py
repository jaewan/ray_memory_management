import random
import ray
import numpy as np
import time
from ray._private.services import get_node_ip_address

@ray.remote(num_cpus=1)
class Generate:
    def __init__(self, actor_id):
        self.actor_id = actor_id

    def generate_data(self, user_id, object_size):
        # Create some random amount of data?
        data = np.zeros(object_size, dtype=np.uint32)
        print(f"Data generated for user {user_id} with object size {object_size} bytes")
        print(f"Actor {self.actor_id} corresponds to node {get_node_ip_address()}")
        return data

def main(args):

    start_time = time.time()

    ray.init(address='auto')

    num_nodes = args.num_nodes

    ray_actors = []
    
    nodes = [node for node in ray.nodes() if node["Alive"]]
    all_nodes = []
    for node in nodes:
        for r in node["Resources"]:
            if "node" in r:
                all_nodes.append(r)

    resources = {f"node{i}": 1 for i in range(1, num_nodes + 1)}
    for i in range(1, num_nodes + 1):
        actor_id = f"node{i}"
        curr_actor = Generate.options(resources={all_nodes[i - 1]: 0.001}).remote(actor_id)
        ray_actors.append(curr_actor)

    # Users are assigned to nodes randomly
    users = [random.randint(1, num_nodes) for _ in range(num_nodes)] # 1 user per node
    # Maybe modify to make even splits?

    # random requests received and stored into list
    results = []
    for i in range(len(users)):
        if i % 2 == 0:
            curr_actor = ray_actors[users[i] - 1]

            num_requests = random.randint(args.num_requests_lower, args.num_requests_upper)

            for _ in range(num_requests):
                object_size = random.randint(args.object_size_lower, args.object_size_upper)
                result = curr_actor.generate_data.remote(users[i], object_size)
                results.append(result)
        if i % 2 == 1:
            curr_actor = ray_actors[users[i] - 1]
            
            for _ in range(16):
                object_size = 1500000000
                result = curr_actor.generate_data.remote(users[i], object_size)
                results.append(result)

    # object id --> data generated
    generated_data = ray.get(results) # blocking operation that waits until all results are obtained

    end_time = time.time()
    time_taken = end_time - start_time

    data_size = sum(data.nbytes for data in generated_data)
    
    throughput = data_size / time_taken
    throughput = throughput / (1024 * 1024)

    # Print throughput
    print(f"Throughput: {throughput}")
    print(f"Time taken: {time_taken}")
    # Print generated data
    for data in generated_data:
        print(data)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Task Distribution Benchmark")

    parser.add_argument("--num-nodes", required=True, type=int)
    parser.add_argument("--num-users", required=True, type=int) 
    parser.add_argument("--num-requests-lower", default=5, type=int)
    parser.add_argument("--num-requests-upper", default=10, type=int)
    parser.add_argument("--object-size-lower", default=200000000, type=int) # 10 MB
    parser.add_argument("--object-size-upper", default=300000000, type=int) # 15 MB
    args = parser.parse_args()
    main(args)
