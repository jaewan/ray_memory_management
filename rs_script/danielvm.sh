# /usr/bin
mode="$1"

if [ "$mode" = "head" ]; then
    echo "starting head node"
    ray start --head --port=6379 --object-manager-port=8076 --object-store-memory=1000000000
elif [ "$mode" = "worker" ]; then
    echo "starting worker node"
    ray start --address='10.182.0.2:6379' --object-manager-port=8076 --object-store-memory=1000000000
else
    echo "unrecognized command"
fi
