package parquetHelper

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.functions.col
import radio.{SimpleSongAggregation, Song}
import spark.SparkHelper

object ParquetService {
  val pathRadioStationSongs = "data/allRadioPartitionByRadioAndDate.parquet"
  val pathRadioES = "data/broadcast.parquet"

  private val spark = SparkHelper.getSparkSession()
  import spark.implicits._

  val schema = new StructType()
    .add("timestamp", TimestampType)
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

  def batchWay() = {
    //Classic  Batch way
    val batchWay =
      spark
        .read
        .schema(ParquetService.schema)
        .parquet(pathRadioStationSongs)
        .where($"artist" === "Drake")
        .groupBy($"radio", $"artist",  $"title")
        .count()
        .orderBy("count")
        .as[Song]

    batchWay.show()

    batchWay
  }

  def streamingWay() : Dataset[SimpleSongAggregation] = {
    spark
      .readStream
      .schema(ParquetService.schema)
      .option("maxFilesPerTrigger", 1000)  // Treat a sequence of files as a stream by picking one file at a time
      .parquet(pathRadioStationSongs)
      .as[Song]
      .where($"artist" === "Drake")
      .groupBy($"radio", $"artist",  $"title")
      .count()
      .as[SimpleSongAggregation]
  }

  def streamEachEvent : Dataset[Song]  = {
    spark
      .readStream
      .schema(ParquetService.schema)
      .option("maxFilesPerTrigger", 1000)  // Treat a sequence of files as a stream by picking one file at a time
      .parquet(pathRadioES)
      .as[Song]
      .where($"artist" === "Drake")
      .withWatermark("timestamp", "10 minutes")
      .as[Song]
  }

  //Process stream on console to debug only
  def debugStream(staticInputDF: DataFrame) = {
    staticInputDF.writeStream
      .format("console")
      .outputMode("complete")
      .queryName("Console - Count number of broadcasts for a title/artist by radio")
      .start()
  }
}
