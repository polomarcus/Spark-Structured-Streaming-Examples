CREATE KEYSPACE IF NOT EXISTS test
  WITH REPLICATION = {
   'class' : 'SimpleStrategy',
   'replication_factor' : 1
  };

CREATE TABLE IF NOT EXISTS test.radio (
  radio varchar,
  title varchar,
  artist varchar,
  count bigint,
  PRIMARY KEY (radio, title, artist)
) WITH comment = 'First sink to test the other "unsafe" Cassandra Foreach Sink.';

CREATE TABLE IF NOT EXISTS test.radioOtherSink (
  radio varchar,
  title varchar,
  artist varchar,
  count bigint,
  PRIMARY KEY (radio, title, artist)
) WITH comment = 'Second sink to test Datastax's connector';

CREATE TABLE IF NOT EXISTS test.kafkaMetadata (
  partition int,
  offset bigint,
  PRIMARY KEY (partition)
) WITH comment = 'Save kafka metadata : topic/partitions' offsets ';