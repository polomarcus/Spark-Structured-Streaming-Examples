#!/bin/bash

screen -dmS "elastic" /home/paul/bigdata/elasticsearch-6.1.0/bin/elasticsearch

sleep 10
screen -dmS "kibana" /home/paul/bigdata/kibana-6.1.0-linux-x86_64/bin/kibana

screen -ls