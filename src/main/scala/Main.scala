package main

import cassandra.CassandraDriver
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import radio.Song


object Main {

  def getSparkSession = {
    val conf = new SparkConf()
      .setAppName("Structured Streaming from Parquet to Cassandra")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.sql.streaming.checkpointLocation", "checkpoint")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    SparkSession
      .builder()
      .getOrCreate()
  }

  def main(args: Array[String]) {
    val pathRadioStationSongs = "data/allRadioPartitionByRadioAndDate.parquet"

    val jsonSchema = new StructType()
      .add("timestamp", IntegerType)
      .add("title", StringType)
      .add("artist", StringType)
      .add("radio", StringType)
      .add("humanDate", LongType)
      .add("hour", IntegerType)
      .add("minute", IntegerType)
      .add("allArtists", StringType)
      .add("year", IntegerType)
      .add("month", IntegerType)
      .add("day", IntegerType)

    val spark = getSparkSession
    import spark.implicits._

    //Classic  Batch way
    println("Batch Results")
    val batchWay =
      spark
        .read
        .schema(jsonSchema)
        .parquet(pathRadioStationSongs)
        .as[Song]
        .where($"artist" === "Drake")
        .groupBy($"radio", $"artist",  $"title")
        .count()

    batchWay.show()

    //Streaming way
    //Read
    val staticInputDF =
    spark
      .readStream
      .schema(jsonSchema)
      .option("maxFilesPerTrigger", 1000)  // Treat a sequence of files as a stream by picking one file at a time
      .parquet(pathRadioStationSongs)
      .as[Song]
      .where($"artist" === "Drake")
      .groupBy($"radio", $"artist",  $"title")
      .count()

    //To debug and query data on the fly
    staticInputDF.createOrReplaceTempView("events")

    //Process stream on console to debug only
    /*val query = staticInputDF.writeStream
      .format("console")
      .outputMode("complete")
      .queryName("Console - Count number of broadcasts for a title/artist by radio")
      .start()*/

    val queryToKafka = staticInputDF
      .select(to_json(struct($"*")).cast(StringType).alias("value"))
      .writeStream
      .outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .queryName("Kafka - Count number of broadcasts for a title/artist by radio")
      .option("topic", "test")
      .start()

    val kafkaInputDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    kafkaInputDF
      .select($"value".cast(StringType)).alias("value") //to avoid using cast to string multiple times
      .select( get_json_object($"value", "$.radio").alias("radio"),
               get_json_object($"value", "$.title").alias("title"),
               get_json_object($"value", "$.artist").alias("artist"),
               get_json_object($"value", "$.count").alias("count")
      )
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    //@TODO write to cassandra
    //CassandraDriver.getTestInfo //check everything is fine

    //https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach
    //CassandraDriver.save(staticInputDS.toDF())

    spark.streams.awaitAnyTermination()
  }
}
