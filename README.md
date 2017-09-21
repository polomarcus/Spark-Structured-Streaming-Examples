# Kafka / Cassandra / Spark Structured Streaming Example
Stream the number of time **Drake is broadcasted** on each radio.
And also, see how easy is Spark Structured Streaming 2.2.0 to use using Spark SQL's Dataframe API

## Run the Project
```
sbt run
```

### Run the project after another time
As checkpointing enables us to process our data exactly once, we need to delete the checkpointing folders to re run our examples.
```
rm -rf checkpoint/
sbt run
```
### Requirements
@TODO docker compose
* Cassandra 3.10 (see below to create the 2 tables the project uses)
* Kafka 0.10+ (with Zookeeper), with one topic "test". See [this Kafka script](https://github.com/polomarcus/Spark-Structured-Streaming-Examples/blob/master/stackScripts/startKafkaStack.sh)


## Input data
Coming from radio stations stored inside a parquet file, the stream is emulated with ` .option("maxFilesPerTrigger", 1)` option.

The stream is after read to be sink into Kafka.
Then, Kafka to Cassandra

## Output data 
Stored inside Kafka and Cassandra for example only.
Cassandra's Sinks uses the [ForeachWriter](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.ForeachWriter) and also the [StreamSinkProvider](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.sources.StreamSinkProvider) to compare both sinks.

One is using the Datastax's Cassandra saveToCassandra method. The other another method, messier (untyped), that uses CQL on a custom foreach loop.

From Spark's doc about batch duration:
> Trigger interval: Optionally, specify the trigger interval. If it is not specified, the system will check for availability of new data as soon as the previous processing has completed. If a trigger time is missed because the previous processing has not completed, then the system will attempt to trigger at the next trigger point, not immediately after the processing has completed.

### Kafka topic
One topic "test" with only one partition

#### Send a message
```
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test 


{"radio":"skyrock","artist":"Drake","title":"Hold On We’Re Going Home","count":38} 
```

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

A 3rd sink to store **kafka metadata** in case checkpointing is not available (application upgrade for example)
```
CREATE TABLE test.kafkaMetadata (
  partition int,
  offset bigint,
  PRIMARY KEY (partition)
);
```

#### Table Content
##### Radio
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

##### Kafka Metadata
When doing an application upgrade, we cannot use [checkpointing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing), so we need to store our offset into a external datasource, here Cassandra is chosen.
Then, when starting our kafka source we need to use the option "StartingOffsets" with a json string like 
```
""" {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """
```
Learn more [in the official Spark's doc for Kafka](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-batch-queries).

In the case, there is not Kafka's metadata stored inside Cassandra, **earliest** is used.

```
cqlsh> SELECT * FROM test.kafkametadata;
 partition | offset
-----------+--------
         0 |    171
```

## Useful links
* [Processing Data in Apache Kafka with Structured Streaming in Apache Spark 2.2](https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html)
* https://databricks.com/blog/2017/04/04/real-time-end-to-end-integration-with-apache-kafka-in-apache-sparks-structured-streaming.html
* https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach
* https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes

## Inspired by
* https://github.com/ansrivas/spark-structured-streaming
* [Holden Karau's High Performance Spark](https://github.com/holdenk/spark-structured-streaming-ml/blob/master/src/main/scala/com/high-performance-spark-examples/structuredstreaming/CustomSink.scala#L66)
* [Jay Kreps blog articles](https://medium.com/@jaykreps/exactly-once-support-in-apache-kafka-55e1fdd0a35f)


