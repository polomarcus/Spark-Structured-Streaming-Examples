#!/bin/bash

docker-compose up -d --no-recreate;

# create Cassandra schema
sleep 5
echo "Creating Cassandra's Schema... if error run ./src/conf/cassandra/create-schema.sh"
./src/conf/cassandra/create-schema.sh
