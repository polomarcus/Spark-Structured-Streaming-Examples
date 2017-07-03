package kafka

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{struct, to_json, _}
import org.apache.spark.sql.types.{StringType, _}
import spark.SparkHelper

object KafkaSink {
  private val spark = SparkHelper.getSparkSession()

  import spark.implicits._

  def writeStream(staticInputDF: DataFrame) = {
    staticInputDF
      .select(to_json(struct($"*")).cast(StringType).alias("value"))
      .writeStream
      .outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaService.bootstrapServers)
      .queryName("Kafka - Count number of broadcasts for a title/artist by radio")
      .option("topic", "test")
      .start()
  }

  //Console sink from Kafka's stream
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
