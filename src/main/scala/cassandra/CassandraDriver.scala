package cassandra

import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import kafka.KafkaService
import radio.{SimpleSongAggregation, Song}
import spark.SparkHelper

object CassandraDriver {
  private val spark = SparkHelper.getSparkSession()
  import spark.implicits._

  val cassandraWriter = new CassandraSinkForeach()

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
  def saveForeach(df: DataFrame) = {
    val ds = CassandraDriver.getDatasetForCassandra(df)

    ds
      .writeStream
      .queryName("KafkaToCassandraForeach")
      .foreach(cassandraWriter)
      .start()
  }

  def save(df: DataFrame) = {
    val ds = CassandraDriver.getDatasetForCassandra(df)

    ds
      .writeStream
      .format("cassandra.CassandraSinkProvider")
      .queryName("KafkaToCassandra")
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
