package kafka

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.SparkHelper

object KafkaService {
  private val spark = SparkHelper.getSparkSession()

  import spark.implicits._

  val bootstrapServers = "localhost:9092"

  def toSink(staticInputDF: DataFrame) = {
    staticInputDF
      .select(to_json(struct($"*")).cast(StringType).alias("value"))
      .writeStream
      .outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .queryName("Kafka - Count number of broadcasts for a title/artist by radio")
      .option("topic", "test")
      .start()
  }

  def fromSink() = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
  }

  def debugStream(staticKafkaInputDF: DataFrame) = {
    staticKafkaInputDF.select($"value".cast(StringType)).alias("value") //to avoid using cast to string multiple times
      .select( get_json_object($"value", "$.radio").alias("radio"),
      get_json_object($"value", "$.title").alias("title"),
      get_json_object($"value", "$.artist").alias("artist"),
      get_json_object($"value", "$.count").alias("count")
    )
      .writeStream
      .option("startingOffsets", "earliest")
      .outputMode("update")
      .format("console")
      .start()
  }
}
