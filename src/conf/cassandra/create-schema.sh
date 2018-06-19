#!/bin/bash

# create Cassandra schema
docker-compose exec cassandra cqlsh -f /schema.cql;

# confirm schema
docker-compose exec cassandra cqlsh -e "DESCRIBE SCHEMA;"