package main

import cassandra.CassandraDriver
import kafka.{KafkaService, KafkaSink, KafkaSource}
import parquetHelper.ParquetService
import spark.SparkHelper

object Main {


  def main(args: Array[String]) {
    val spark = SparkHelper.getAndConfigureSparkSession()

    //Classic Batch
    ParquetService.batchWay()

    //Stream
    val staticInputDF = ParquetService.streamingWay()

    //Stream To Kafka
    val queryToKafka = KafkaSink.writeStream(staticInputDF)

    //Read from Kafka
    val kafkaInputDF = KafkaSource.read()

    //Debug Kafka input Stream
    KafkaSink.debugStream(kafkaInputDF)

    CassandraDriver.getTestInfo()
    //Saving using the foreach method
    CassandraDriver.saveForeach(kafkaInputDF)

    //Saving using Datastax connector's saveToCassandra method
    CassandraDriver.saveStreamSinkProvider(kafkaInputDF)

    //@TODO debug
    CassandraDriver.debug()

    spark.streams.awaitAnyTermination()
  }
}
