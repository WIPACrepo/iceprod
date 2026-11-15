#!/bin/bash

podman run --rm -d --name mongo --network=host mongo:8 --replSet rs0 --bind_ip_all --port 27017

sleep 1

# use localhost as the member host, so clients don't switch to the machine hostname
mongosh --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]}).ok"

sleep 1

mongosh --eval "rs.status().ok"

echo "ready"

( trap exit SIGINT ; read -r -d '' _ </dev/tty ) ## wait for Ctrl-C

podman kill mongo

echo "mongo is dead"
