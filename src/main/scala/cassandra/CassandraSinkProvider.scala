package cassandra

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._

//@TODO : https://databricks.com/blog/2017/04/04/real-time-end-to-end-integration-with-apache-kafka-in-apache-sparks-structured-streaming.html

//From Holden Karau's https://github.com/holdenk/spark-structured-streaming-ml/blob/master/src/main/scala/com/high-performance-spark-examples/structuredstreaming/CustomSink.scala
class CassandraSinkProvider extends StreamSinkProvider {
  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    new CassandraSink()
  }
}


//@TODO fix me
class CassandraSink extends Sink {

  /*
   * As per SPARK-16020 arbitrary transformations are not supported, but
   * converting to an RDD allows us to do magic.
   */
  override def addBatch(batchId: Long, df: DataFrame) = {
    println(s"saveToCassandra batchId : ${batchId}")

    //df.withColumn("batchId", lit(batchId)) //Add batch Id column to debug, and might be used for idempotence
    df.printSchema()
    df.show()

    df.rdd.saveToCassandra("test", "radio", SomeColumns("title", "artist", "radio", "count"))
  }
}
