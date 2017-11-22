package main

import cassandra.CassandraDriver
import kafka.{KafkaSink, KafkaSource}
import parquetHelper.ParquetService
import spark.SparkHelper

object Main {

  def main(args: Array[String]) {
    val spark = SparkHelper.getAndConfigureSparkSession()

    //Classic Batch
    //ParquetService.batchWay()

    //Generate a "fake" stream from a parquet file
    val staticInputDS = ParquetService.streamingWay()

    //Send it to Kafka for our example
    KafkaSink.writeStream(staticInputDS)

    //Finally read it from kafka, in case checkpointing is not available we read last offsets saved from Cassandra
    val (startingOption, partitionsAndOffsets) = CassandraDriver.getKafaMetadata()
    val kafkaInputDS = KafkaSource.read(startingOption, partitionsAndOffsets)

    //Just debugging Kafka source into our console
    KafkaSink.debugStream(kafkaInputDS)

    //Saving using Datastax connector's saveToCassandra method
    CassandraDriver.saveStreamSinkProvider(kafkaInputDS)

    //Saving using the foreach method
    //CassandraDriver.saveForeach(kafkaInputDS) //Untype/unsafe method using CQL  --> just here for example

    //Wait for all streams to finish
    spark.streams.awaitAnyTermination()
  }
}
