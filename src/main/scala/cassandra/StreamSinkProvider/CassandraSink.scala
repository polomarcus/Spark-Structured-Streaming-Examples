package cassandra.StreamSinkProvider

import cassandra.{CassandraDriver, CassandraKafkaMetadata}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.functions.max
import spark.SparkHelper
import cassandra.CassandraDriver
import com.datastax.spark.connector._
import kafka.KafkaMetadata
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.types.LongType
import radio.SimpleSongAggregation

/**
* must be idempotent and synchronous (@TODO check asynchronous/synchronous from Datastax's Spark connector) sink
*/
class CassandraSink() extends Sink {
  private val spark = SparkHelper.getSparkSession()
  import spark.implicits._
  import org.apache.spark.sql.functions._

  private def saveToCassandra(df: DataFrame) = {
    val ds = CassandraDriver.getDatasetForCassandra(df)
    ds.show() //Debug only

    ds.rdd.saveToCassandra(CassandraDriver.namespace,
      CassandraDriver.StreamProviderTableSink,
      SomeColumns("title", "artist", "radio", "count")
    )

    saveKafkaMetaData(df)
  }

  /*
   * As per SPARK-16020 arbitrary transformations are not supported, but
   * converting to an RDD allows us to do magic.
   */
  override def addBatch(batchId: Long, df: DataFrame) = {
    println(s"CassandraSink - Datastax's saveToCassandra method -  batchId : ${batchId}")
    saveToCassandra(df)
  }

  /**
    * saving the highest value of offset per partition when checkpointing is not available (application upgrade for example)
    * http://docs.datastax.com/en/cassandra/3.0/cassandra/dml/dmlTransactionsDiffer.html
    * should be done in the same transaction as the data linked to the offsets
    */
  private def saveKafkaMetaData(df: DataFrame) = {
    val kafkaMetadata = df
      .groupBy($"partition")
      .agg(max($"offset").cast(LongType).as("offset"))
      .as[KafkaMetadata]

    println("Saving Kafka Metadata (partition and offset per topic (only one in our example)")
    kafkaMetadata.show()

    kafkaMetadata.rdd.saveToCassandra(CassandraDriver.namespace,
      CassandraDriver.kafkaMetadata,
      SomeColumns("partition", "offset")
    )

    //Otherway to save offset inside Cassandra
    //kafkaMetadata.collect().foreach(CassandraKafkaMetadata.save)
  }
}

