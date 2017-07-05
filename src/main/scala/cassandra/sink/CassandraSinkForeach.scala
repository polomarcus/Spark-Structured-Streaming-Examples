package cassandra.sink

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.ForeachWriter
import radio.SimpleSongAggregation
import spark.SparkHelper

/**
  * Inspired by
  * https://github.com/ansrivas/spark-structured-streaming/
  * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach
  */
class CassandraSinkForeach() extends ForeachWriter[SimpleSongAggregation] {
  private val connector = CassandraConnector(SparkHelper.getSparkSession().sparkContext.getConf)

  private def cql(record: SimpleSongAggregation): String = s"""
       insert into test.radio (title, artist, radio, count)
       values('${record.title}', '${record.artist}', '${record.radio}', ${record.count})"""

  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    //@TODO command to check if cassandra cluster is up
    true
  }

  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/1_connecting.md#connection-pooling
  def process(record: SimpleSongAggregation) = {
    println(s"Saving record: $record")
    connector.withSessionDo(session =>
      session.execute(cql(record))
    )
  }

  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md#cassandra-connection-parameters

  def close(errorOrNull: Throwable): Unit = {
    // close the connection
    //connection.keep_alive_ms	--> 5000ms :	Period of time to keep unused connections open
  }
}