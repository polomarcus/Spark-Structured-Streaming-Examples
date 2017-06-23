package kafka

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.SparkHelper

object KafkaService {
  private val spark = SparkHelper.getSparkSession()

  import spark.implicits._

  val radioStructureName = "radioCount"

  val bootstrapServers = "localhost:9092"

  val schemaOutput = new StructType()
    .add("title", StringType)
    .add("artist", StringType)
    .add("radio", StringType)
    .add("count", LongType)

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

  /**
    * will return, we keep kafka metadata for our example, otherwise we would only focus on "radioCount" structure
     |-- key: binary (nullable = true)
     |-- value: binary (nullable = true)
     |-- topic: string (nullable = true)
     |-- partition: integer (nullable = true)
     |-- offset: long (nullable = true)
     |-- timestamp: timestamp (nullable = true)
     |-- timestampType: integer (nullable = true)
     |-- radioCount: struct (nullable = true)
     |    |-- title: string (nullable = true)
     |    |-- artist: string (nullable = true)
     |    |-- radio: string (nullable = true)
     |    |-- count: long (nullable = true)

    * @return
    */
  def fromSink() = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load() //--> Partition, offsets, value, key etc.
      .withColumn(radioStructureName, // nested structure with our json
        from_json($"value".cast(StringType), schemaOutput)
      ) //From binary to JSON object
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
