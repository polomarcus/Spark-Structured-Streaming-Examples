package cassandra.sink

import cassandra.CassandraDriver
import com.datastax.spark.connector._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}
import spark.SparkHelper

/**
  From Holden Karau's High Performance Spark
  https://github.com/holdenk/spark-structured-streaming-ml/blob/master/src/main/scala/com/high-performance-spark-examples/structuredstreaming/CustomSink.scala#L66
  *
  */
class CassandraSinkProvider extends StreamSinkProvider {
  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): CassandraSink = {
    new CassandraSink()
  }
}

/**
  * must be idempotent and synchronous (@TODO check asynchronous/synchronous from Datastax's Spark connector) sink
  */
class CassandraSink() extends Sink {
  private val spark = SparkHelper.getSparkSession()
  import spark.implicits._
  import org.apache.spark.sql.functions._

  private def saveToCassandra(df: DataFrame) = {
    println("Saving this DF to Cassandra")
    df.show() //Debug only

    df.select($"title",$"artist",$"radio",$"count").rdd.saveToCassandra(CassandraDriver.namespace,
      CassandraDriver.StreamProviderTableSink,
      SomeColumns("title", "artist", "radio", "count")
    )

    saveKafkaMetaData(df) //@TODO should be in the same transaction than the previous operation
  }

  /*
   * As per SPARK-16020 arbitrary transformations are not supported, but
   * converting to an RDD allows us to do magic.
   */
  override def addBatch(batchId: Long, df: DataFrame) = {
    println(s"saveToCassandra batchId : ${batchId}")
    saveToCassandra(df)
  }

  /**
    * saving the highest value of offset per partition when checkpointing is not available (application upgrade for example)
    * @TODO should be done in the same transaction as the data linked to the offsets
    */
  private def saveKafkaMetaData(df: DataFrame) = {
    val kafkaMetadata = df.groupBy($"partition").agg(max("$offset"))

    kafkaMetadata.show()

    kafkaMetadata.rdd.saveToCassandra(CassandraDriver.namespace,
      CassandraDriver.kafkaMetadata,
      SomeColumns("partition", "offset")
    )
  }
}
