package cassandra

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import radio.Song

object CassandraDriver {

  val spark = SparkSession
    .builder()
    .getOrCreate()

  def getTestInfo() = {
    val rdd = spark.sparkContext.cassandraTable("test", "kv")
    println(rdd.count)
    println(rdd.first)
    println(rdd.map(_.getInt("value")).sum)
  }

  def save(df: DataFrame) = {
    df.rdd.saveToCassandra("test", "radio", SomeColumns("radio", "title", "artist", "count"))
  }
}
