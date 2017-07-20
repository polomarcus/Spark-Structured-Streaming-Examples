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
    val staticInputDF = ParquetService.streamingWay()

    //Send it to Kafka for our example
    val queryToKafka = KafkaSink.writeStream(staticInputDF)

    //Finally read it from kafka, in case checkpointing is not available we read last offsets saved from Cassandra
    val (startingOption, partitionsAndOffsets) = CassandraDriver.getKafaMetadata()
    val kafkaInputDF = KafkaSource.read(startingOption, partitionsAndOffsets)

    //Just debugging Kafka source into our console
    KafkaSink.debugStream(kafkaInputDF)

    //Saving using Datastax connector's saveToCassandra method
    CassandraDriver.saveStreamSinkProvider(kafkaInputDF)

    //Saving using the foreach method
    //CassandraDriver.saveForeach(kafkaInputDF) //Untype/unsafe method using CQL  --> just here for example

    //Wait for all streams to finish
    spark.streams.awaitAnyTermination()
  }
}
