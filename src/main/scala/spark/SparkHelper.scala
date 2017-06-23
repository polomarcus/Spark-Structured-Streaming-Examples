package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkHelper {
  def getAndConfigureSparkSession() = {
    val conf = new SparkConf()
      .setAppName("Structured Streaming from Parquet to Cassandra")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.sql.streaming.checkpointLocation", "checkpoint")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR") //@TODO not working for block manager

    SparkSession
      .builder()
      .getOrCreate()
  }

  def getSparkSession() = {
    SparkSession
      .builder()
      .getOrCreate()
  }
}
