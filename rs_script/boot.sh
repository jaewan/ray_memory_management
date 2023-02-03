#!/bin/bash
mode="$1"
head_ip="$2"
listener="$3"
port="$4"

if [[ ! listener ]]; then
    listener="7001"
fi

if [[ ! $port ]]; then
    port="6379"
fi

if [ "$mode" = "head" ]; then
    ip=""
    # get ips
    if [[ "$OSTYPE" = "darwin"* ]]; then # macOS
        ip=$(ipconfig getifaddr en0)
        echo "Started head node on: $ip"
    # TODO: add linux configs to find IP.
    fi

    # start ray 
    ray start --head --port=$port --ray-client-server-port=$listener --node-ip-address=$ip
elif [ "$mode" = "worker" ]; then
    adrs="$head_ip:$port"
    echo "Connecting worker to: $adrs"
    ray start --address=$adrs
else
    echo "Unknown mode: select head or worker."
fi
