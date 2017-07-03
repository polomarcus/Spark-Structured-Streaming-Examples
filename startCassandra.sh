#!/bin/bash

#Cassandra 3.10
screen -dmS "Cassandra" /home/paul/bigdata/apache-cassandra-3.10/bin/cassandra
sleep 10
/home/paul/bigdata/apache-cassandra-3.10/bin/cqlsh

