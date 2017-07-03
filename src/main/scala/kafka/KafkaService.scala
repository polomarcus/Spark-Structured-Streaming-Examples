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
}
