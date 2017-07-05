# Kafka / Cassandra / Spark Structured Streaming Example
Stream the number of time **Drake is broadcasted** on each radio.
And also, see how easy is Spark Structured Streaming to use using SparkSQL

## Input data
Coming from radio stations stored inside a parquet file, the stream is emulated with ` .option("maxFilesPerTrigger", 1)` option.

The stream is after read to be sink into Kafka.
Then, Kafka to Cassandra


## Output data 
Stored inside Kafka and Cassandra for example only.
Cassandra's Sinks uses the [ForeachWriter](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.ForeachWriter) and also the [StreamSinkProvider](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.sources.StreamSinkProvider) to compare both sinks.
One is using the Datastax's Cassandra saveToCassandra method. The other another method, more messy, that uses CQL on a custom foreach loop.
### Kafka topic
topic:test
### Cassandra Table
A table for the ForeachWriter
```
CREATE TABLE test.radio (
  radio varchar,
  title varchar,
  artist varchar,
  count bigint,
  PRIMARY KEY (radio, title, artist)
);
```

A second sink to test the other writer.
```
CREATE TABLE test.radioOtherSink (
  radio varchar,
  title varchar,
  artist varchar,
  count bigint,
  PRIMARY KEY (radio, title, artist)
);
```


```
cqlsh> SELECT * FROM test.radio;

 radio   | title                    | artist | count
---------+--------------------------+--------+-------
 skyrock |                Controlla |  Drake |     1
 skyrock |                Fake Love |  Drake |     9
 skyrock | Hold On We’Re Going Home |  Drake |    35
 skyrock |            Hotline Bling |  Drake |  1052
 skyrock |  Started From The Bottom |  Drake |    39
    nova |         4pm In Calabasas |  Drake |     1
    nova |             Feel No Ways |  Drake |     2
    nova |                From Time |  Drake |    34
    nova |                     Hype |  Drake |     2

```

## Useful links
* https://databricks.com/blog/2017/04/04/real-time-end-to-end-integration-with-apache-kafka-in-apache-sparks-structured-streaming.html
* https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach
* https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes

## Inspired by
* https://github.com/ansrivas/spark-structured-streaming
* From Holden Karau's High Performance Spark : https://github.com/holdenk/spark-structured-streaming-ml/blob/master/src/main/scala/com/high-performance-spark-examples/structuredstreaming/CustomSink.scala#L66

## Requirements
* Cassandra 3.10
* Kafka 0.10+ (with Zookeeper)

