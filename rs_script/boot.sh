#!/bin/bash
mode="$1"
head_ip="$2"
port="$3"

if [[ ! $port ]]; then
    port="6379"
fi

if [ "$mode" = "head" ]; then
    ray start --head --port=$port --ray-client-server-port=7001 --node-ip-address=$(ipconfig getifaddr en0)
    # get ips
    if [[ "$OSTYPE" = "darwin"* ]]; then # macOS
        ip=$(ipconfig getifaddr en0)
        echo "Started head node on: $ip"
    # TODO: add linux configs to find IP.
    fi
elif [ "$mode" = "worker" ]; then
    adrs="$head_ip:$port"
    echo "Connecting worker to: $adrs"
    ray start --address=$adrs
else
    echo "Unknown mode: select head or worker."
fi
