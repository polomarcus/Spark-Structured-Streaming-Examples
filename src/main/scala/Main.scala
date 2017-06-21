package main

import cassandra.CassandraDriver
import kafka.KafkaService
import parquetHelper.ParquetService
import spark.SparkHelper

object Main {


  def main(args: Array[String]) {
    val spark = SparkHelper.getAndConfigureSparkSession()
    import spark.implicits._

    //Batch
    ParquetService.batchWay()

    //Stream
    val staticInputDF = ParquetService.streamingWay()

    //Stream To Kafka
    val queryToKafka = KafkaService.toSink(staticInputDF)

    //Read from Kafka
    val kafkaInputDF = KafkaService.fromSink()

    //Debug Stream
    KafkaService.debugStream(kafkaInputDF)

    //@TODO write to cassandra
    //CassandraDriver.getTestInfo //check everything is fine

    //https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach
    //CassandraDriver.save(staticInputDS.toDF())

    spark.streams.awaitAnyTermination()
  }
}
