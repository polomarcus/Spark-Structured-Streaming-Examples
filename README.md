# Kafka / Cassandra / Spark Structured Streaming Example

## Input data
Coming from radio stations stored inside a parquet file, the stream is emulated with ` .option("maxFilesPerTrigger", 1)` option.

## Output data
Stored inside Kafka and Cassandra for example only

```
./cqlsh
SELECT * FROM test.kv;
```

## Requirements
* Cassandra
* Kafka 0.10+ (with Zookeeper)