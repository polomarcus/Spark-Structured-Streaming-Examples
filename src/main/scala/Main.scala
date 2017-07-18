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

    //Stream
    val staticInputDF = ParquetService.streamingWay()

    //Stream To Kafka
    val queryToKafka = KafkaSink.writeStream(staticInputDF)

    //Read from Kafka
    //@TODO /!\ synchronously
    val (startingOption, partitionsAndOffsets) = CassandraDriver.getKafaMetadata()
    val kafkaInputDF = KafkaSource.read(startingOption, partitionsAndOffsets)

    //Debug Kafka input Stream
    KafkaSink.debugStream(kafkaInputDF)

    CassandraDriver.getTestInfo()

    //Saving using the foreach method
    //CassandraDriver.saveForeach(kafkaInputDF) //Untype/unsafe method using CQL  --> just here for example

    //Saving using Datastax connector's saveToCassandra method
    CassandraDriver.saveStreamSinkProvider(kafkaInputDF)

    spark.streams.awaitAnyTermination()
  }
}
