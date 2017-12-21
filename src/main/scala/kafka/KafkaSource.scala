package kafka

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{struct, to_json, _}
import org.apache.spark.sql.types.{StringType, _}
import radio.{SimpleSongAggregation, SimpleSongAggregationKafka}
import spark.SparkHelper

/**
 @see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
 */
object KafkaSource {
  private val spark = SparkHelper.getSparkSession()

  import spark.implicits._

  /**
    * will return, we keep some kafka metadata for our example, otherwise we would only focus on "radioCount" structure
     |-- key: binary (nullable = true)
     |-- value: binary (nullable = true)
     |-- topic: string (nullable = true) : KEPT
     |-- partition: integer (nullable = true) : KEPT
     |-- offset: long (nullable = true) : KEPT
     |-- timestamp: timestamp (nullable = true) : KEPT
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
    def read(startingOption: String = "startingOffsets", partitionsAndOffsets: String = "earliest") : Dataset[SimpleSongAggregationKafka] = {
      println("Reading from Kafka")

      spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", KafkaService.topicName)
      .option("enable.auto.commit", false) // Cannot be set to true in Spark Strucutured Streaming https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#kafka-specific-configurations
      .option("group.id", "Structured-Streaming-Examples")
      .option("failOnDataLoss", false) // when starting a fresh kafka (default location is temporary (/tmp) and cassandra is not (var/lib)), we have saved different offsets in Cassandra than real offsets in kafka (that contains nothing)
      .option(startingOption, partitionsAndOffsets) //this only applies when a new query is started and that resuming will always pick up from where the query left off
      .load()
      .withColumn(KafkaService.radioStructureName, // nested structure with our json
        from_json($"value".cast(StringType), KafkaService.schemaOutput) //From binary to JSON object
      ).as[SimpleSongAggregationKafka]
      .filter(_.radioCount != null) //TODO find a better way to filter bad json
  }
}
