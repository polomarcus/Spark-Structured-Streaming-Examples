CREATE KEYSPACE IF NOT EXISTS structuredstreaming
  WITH REPLICATION = {
   'class' : 'SimpleStrategy',
   'replication_factor' : 1
  };


CREATE TABLE IF NOT EXISTS structuredstreaming.radio (
radio varchar,
title varchar,
artist varchar,
count bigint,
PRIMARY KEY (radio, title, artist)
) WITH comment = 'First sink to test the other "unsafe" Cassandra Foreach Sink.';


CREATE TABLE IF NOT EXISTS structuredstreaming.radioOtherSink (
radio varchar,
title varchar,
artist varchar,
count bigint,
PRIMARY KEY (radio, title, artist)
) WITH comment = 'Second sink to test Datastax connector.';

CREATE TABLE IF NOT EXISTS structuredstreaming.kafkaMetadata (
partition int,
offset bigint,
PRIMARY KEY (partition)
) WITH comment = 'Save kafka metadata : topic and partitions offsets.';