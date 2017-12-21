package kafka

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{struct, to_json, _}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{StringType, _}
import radio.{SimpleSongAggregation, SimpleSongAggregationKafka}
import spark.SparkHelper

object KafkaSink {
  private val spark = SparkHelper.getSparkSession()

  import spark.implicits._

  def writeStream(staticInputDS: Dataset[SimpleSongAggregation]) : StreamingQuery = {
    println("Writing to Kafka")
    staticInputDS
      .select(to_json(struct($"*")).cast(StringType).alias("value"))
      .writeStream
      .outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaService.bootstrapServers)
      .queryName("Kafka - Count number of broadcasts for a title/artist by radio")
      .option("topic", "test")
      .start()
  }

  /**
      Console sink from Kafka's stream
      +----+--------------------+-----+---------+------+--------------------+-------------+--------------------+
      | key|               value|topic|partition|offset|           timestamp|timestampType|          radioCount|
      +----+--------------------+-----+---------+------+--------------------+-------------+--------------------+
      |null|[7B 22 72 61 64 6...| test|        0|    60|2017-11-21 22:56:...|            0|[Feel No Ways,Dra...|
    *
    */
  def debugStream(staticKafkaInputDS: Dataset[SimpleSongAggregationKafka]) = {
    staticKafkaInputDS
      .writeStream
      .queryName("Debug Stream Kafka")
      .format("console")
      .start()
  }
}
