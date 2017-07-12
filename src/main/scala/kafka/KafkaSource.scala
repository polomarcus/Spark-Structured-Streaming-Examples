package kafka

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{struct, to_json, _}
import org.apache.spark.sql.types.{StringType, _}
import spark.SparkHelper

/**
 @see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
 */
object KafkaSource {
  private val spark = SparkHelper.getSparkSession()

  import spark.implicits._

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
    *
    *
    * startingOffsets should use a JSON coming from the lastest offsets saved in our DB (Cassandra here)
    */
  def read(startingOption: String = "startingOffsets", partitionsAndOffsets: String = "earliest") = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", KafkaService.topicName)
      .option(startingOption, partitionsAndOffsets) //this only applies when a new query is started and that resuming will always pick up from where the query left off
      .load()
      .withColumn(KafkaService.radioStructureName, // nested structure with our json
        from_json($"value".cast(StringType), KafkaService.schemaOutput)
      ) //From binary to JSON object
  }
}
