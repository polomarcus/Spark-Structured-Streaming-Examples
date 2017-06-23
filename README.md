# Kafka / Cassandra / Spark Structured Streaming Example

## Input data
Coming from radio stations stored inside a parquet file, the stream is emulated with ` .option("maxFilesPerTrigger", 1)` option.

The stream is after read to be sink into Kafka.
Then, Kafka to Cassandra


## Output data
Stored inside Kafka and Cassandra for example only

### Kafka topic
topic:test
### Cassandra Table
```
CREATE TABLE test.radio (
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

## Requirements
* Cassandra 3.10
* Kafka 0.10+ (with Zookeeper)

