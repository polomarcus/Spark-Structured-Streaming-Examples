#!/bin/bash

sudo docker-compose exec kafka  \
kafka-topics --create --topic test --partitions 1 --replication-factor 1 --if-not-exists --zookeeper localhost:32181