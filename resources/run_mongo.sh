#!/bin/bash

podman run --rm -d --name mongo --network=host mongo:8 --replSet rs0 --bind_ip_all --port 27017

sleep 1

mongosh --eval "rs.initiate().ok"

sleep 1

mongosh --eval "rs.status().ok"

echo "ready"

( trap exit SIGINT ; read -r -d '' _ </dev/tty ) ## wait for Ctrl-C

podman kill mongo

echo "mongo is dead"
