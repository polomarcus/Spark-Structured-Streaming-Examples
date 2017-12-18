package parquetHelper

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.window
import radio.{SimpleSongAggregation, Song}
import spark.SparkHelper

object ParquetService {
  val pathRadioStationSongs = "data/allRadioPartitionByRadioAndDate.parquet"

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
        .as[Song]
        .where($"artist" === "Drake")
        .groupBy($"radio", $"artist",  $"title")
        .count()
        .orderBy("count")

    batchWay.show()
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

  def streamEachEvent : DataFrame = {
    spark
      .readStream
      .schema(ParquetService.schema)
      .option("maxFilesPerTrigger", 20)  // Treat a sequence of files as a stream by picking one file at a time
      .parquet(pathRadioStationSongs)
      .as[Song]
      .where($"artist" === "Drake")
      .groupBy( $"timestamp", $"radio", $"artist",  $"title")
      .count()
      //.as[SimpleSongAggregation]
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
