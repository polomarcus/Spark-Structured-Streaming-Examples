package cassandra

import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import kafka.KafkaService
import radio.SimpleSongAggregation
import spark.SparkHelper
import sink._

object CassandraDriver {
  private val spark = SparkHelper.getSparkSession()
  import spark.implicits._

  val namespace = "test"
  val foreachTableSink = "radio"
  val StreamProviderTableSink = "radioOtherSink"

  def getTestInfo() = {
    val rdd = spark.sparkContext.cassandraTable("test", "kv")
    println(rdd.count)
    println(rdd.first)
    println(rdd.map(_.getInt("value")).sum)
  }


  /**
    * remove kafka metadata and only focus on business structure
    */
  private def getDatasetForCassandra(df: DataFrame) = {
    df.select(KafkaService.radioStructureName + ".*").as[SimpleSongAggregation]
  }

  //https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach
  //https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes
  def saveForeach(df: DataFrame) = {
    val ds = CassandraDriver.getDatasetForCassandra(df)

    ds
      .writeStream
      .queryName("KafkaToCassandraForeach")
      .format("update")
      .foreach(new CassandraSinkForeach())
      .start()
  }

  def saveStreamSinkProvider(df: DataFrame) = {
    df
      .writeStream
      .format("cassandra.sink.CassandraSinkProvider")
      .queryName("KafkaToCassandraStreamSinkProvider")
      .format("update") //@TODO check how to handle this in a custom StreakSnkProvider
      .start()
  }

  def debug() = {
   val output = spark.sparkContext.cassandraTable("test", "radio")

    println(output.count)
    /*  output
     .select("radio, artist, title, count")
     .take(10).foreach(println)*/
  }
}
