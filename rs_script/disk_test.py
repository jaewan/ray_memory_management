import os

for _ in range(0, 50):
    os.system('python -m ray.experimental.shuffle --ray-address="10.138.0.2:6379" --num-partitions=2 --partition-size=75e8')