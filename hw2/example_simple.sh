#!/bin/bash

# resetting
docker compose down

# containers setup
docker compose up -d node_1

sleep 3

docker compose up -d node_2 node_3

sleep 3

# test

echo "upserting (test, 123)"

curl -X POST '172.20.0.101:80/upsert?key=test&value=123'

curl '172.20.0.101:80/read?key=test'
