package elastic

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import radio.{SimpleSongAggregation, Song}
import org.elasticsearch.spark.sql.streaming._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.sql.streaming.EsSparkSqlStreamingSink

object ElasticSink {
  def writeStream(ds: Dataset[Song] ) : StreamingQuery = {
    ds   //Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark
      .writeStream
      .outputMode(OutputMode.Append) //Only mode for ES
      .format("org.elasticsearch.spark.sql") //es
      .queryName("ElasticSink")
      .start("test/broadcast") //ES index
  }

}
