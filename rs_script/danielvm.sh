# /usr/bin
mode="$1"

if [ "$mode" = "head" ]; then
    echo "starting head node"
    ray start --head --port=6379 \
              --object-manager-port=8076 \
              --object-store-memory=1000000000
elif [ "$mode" = "worker" ]; then
    echo "starting worker node"
    ray start --address='10.182.0.2:6379' \
              --object-manager-port=8076 \
              --object-store-memory=1000000000
elif [ "$mode" = "run" ]; then
    echo "running shuffle"
    python -m ray.experimental.shuffle --ray-address='10.182.0.2:6379' \
                                       --num-partitions=50 \
                                       --partition-size=200e6 \
                                       --object-store-memory=1e9
else
    echo "unrecognized command"
fi
