package kafka

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{struct, to_json, _}
import org.apache.spark.sql.types.{StringType, _}
import spark.SparkHelper

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
    */
  def read() = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load() //--> Partition, offsets, value, key etc.
      .withColumn(KafkaService.radioStructureName, // nested structure with our json
        from_json($"value".cast(StringType), KafkaService.schemaOutput)
      ) //From binary to JSON object
  }
}
