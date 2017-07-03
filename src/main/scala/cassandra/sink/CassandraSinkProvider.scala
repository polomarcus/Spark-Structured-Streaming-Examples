package cassandra.sink

import cassandra.CassandraDriver
import com.datastax.spark.connector._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}

//From Holden Karau's https://github.com/holdenk/spark-structured-streaming-ml/blob/master/src/main/scala/com/high-performance-spark-examples/structuredstreaming/CustomSink.scala#L66
abstract class CassandraSinkProvider extends StreamSinkProvider {
  //@TODO Provided process must consume the dataset (e.g. call `foreach` or `collect`).
  def process(df: DataFrame): Unit

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): CassandraSink = {
    new CassandraSink()
  }
}


/**
  * idempotent and synchronous (@TODO check asynchronous/synchronous from Datastax's Spark connector) sink
  */
class CassandraSink() extends Sink {
  def saveToCassandra(df: DataFrame) = {
    df.printSchema()
    df.show()
    df.rdd.saveToCassandra(CassandraDriver.namespace, CassandraDriver.StreamProviderTableSink, SomeColumns("title", "artist", "radio", "count"))
  }

  /*
   * As per SPARK-16020 arbitrary transformations are not supported, but
   * converting to an RDD allows us to do magic.
   */
  override def addBatch(batchId: Long, df: DataFrame) = {
    println(s"saveToCassandra batchId : ${batchId}")
    saveToCassandra(df)
  }
}
