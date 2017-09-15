package cassandra.foreachSink

import cassandra.CassandraDriver
import org.apache.spark.sql.ForeachWriter
import radio.SimpleSongAggregation

/**
  * Inspired by
  * https://github.com/ansrivas/spark-structured-streaming/
  * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach
  */
class CassandraSinkForeach() extends ForeachWriter[SimpleSongAggregation] {
  private def cqlRadio(record: SimpleSongAggregation): String = s"""
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
    CassandraDriver.connector.withSessionDo(session =>
      session.execute(cqlRadio(record))
    )
  }

  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md#cassandra-connection-parameters

  def close(errorOrNull: Throwable): Unit = {
    // close the connection
    //connection.keep_alive_ms	--> 5000ms :	Period of time to keep unused connections open
  }
}