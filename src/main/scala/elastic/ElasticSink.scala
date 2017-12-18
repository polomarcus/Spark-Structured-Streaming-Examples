package elastic

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import radio.{SimpleSongAggregation, Song}
import org.elasticsearch.spark.sql.streaming._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.sql.streaming.EsSparkSqlStreamingSink
import org.elasticsearch.spark.sql.streaming.SparkSqlStreamingConfigs
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object ElasticSink {
  def writeStream(ds: DataFrame) : StreamingQuery = {
    ds.printSchema()

    ds
      .withWatermark("timestamp", "10 minutes")   //Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark
      .writeStream
      .option("checkpointLocation", "/save/location")
      .outputMode(OutputMode.Append) //Only mode for ES
      .format("es")
      .queryName("ElasticSink")
      .start("test/broadcast") //ES index
  }

}
