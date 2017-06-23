#kafka_2.11-0.10.2.0
screen -dmS "kafkaZK" /home/paul/bigdata/kafka_2.11-0.10.2.0/bin/zookeeper-server-start.sh /home/paul/bigdata/kafka_2.11-0.10.2.0/config/zookeeper.properties
/home/paul/bigdata/kafka_2.11-0.10.2.0/bin/kafka-server-start.sh /home/paul/bigdata/kafka_2.11-0.10.2.0/config/server.properties
/home/paul/bigdata/kafka_2.11-0.10.2.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
./bin/kafka-console-consumer.sh --topic test --zookeeper localhost --from-beginning
