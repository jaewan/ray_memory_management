#!/bin/bash
mode="$1"       # head or worker node. 
head_ip="$2"
#listener="$2"   # listening port. 
#port="$3"       # other port. 

#if [[ ! $listener ]]; then
    # random port. Ray's default is 10001, but
    # this doesn't work on Berkeley's wifi...
listener="10001"
#fi

#if [[ ! $port ]]; then
port="6379" # ray's default value. 
#fi

if [ "$mode" = "head" ]; then
    ip=""
    # get ips
    # macOS
    if [[ "$OSTYPE" = "darwin"* ]]; then
        ip=$(ipconfig getifaddr en0)
    fi
    # TODO: add linux configs to find IP.
    # start ray
    ray start --head --port=$port \
              --ray-client-server-port=$listener \
              --node-ip-address=$ip \
              --object-store-memory=$(dc -e "10 9 ^ p")
    echo "Started head node on: $ip:$listener"
elif [ "$mode" = "worker" ]; then
    adrs="$head_ip:$port"
    echo "Connecting worker to: $adrs"
    ray start --address=$adrs \
              --object-store-memory=$(dc -e "10 9 ^ p")
else
    echo "Unknown mode: select head or worker."
fi
