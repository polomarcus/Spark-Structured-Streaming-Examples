package main

import cassandra.CassandraDriver
import kafka.KafkaService
import parquetHelper.ParquetService
import spark.SparkHelper

object Main {


  def main(args: Array[String]) {
    val spark = SparkHelper.getAndConfigureSparkSession()

    //Batch
    ParquetService.batchWay()

    //Stream
    val staticInputDF = ParquetService.streamingWay()

    //Stream To Kafka
    val queryToKafka = KafkaService.toSink(staticInputDF)

    //Read from Kafka
    val kafkaInputDF = KafkaService.fromSink()

    //Debug Kafka input Stream
    KafkaService.debugStream(kafkaInputDF)

    CassandraDriver.getTestInfo //check everything is fine

    //Saving using the foreach method
    CassandraDriver.saveForeach(kafkaInputDF)

    //Saving using Datastax connector's saveToCassandra method
    //@TODO CassandraDriver.save(kafkaInputDF)

    //@TODO debug
    CassandraDriver.debug()

    spark.streams.awaitAnyTermination()
  }
}
