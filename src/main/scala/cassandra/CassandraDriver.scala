package cassandra

import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import kafka.{KafkaMetadata, KafkaService}
import radio.SimpleSongAggregation
import spark.SparkHelper
import sink._

object CassandraDriver {
  private val spark = SparkHelper.getSparkSession()
  import spark.implicits._

  val connector = CassandraConnector(SparkHelper.getSparkSession().sparkContext.getConf)

  val namespace = "test"
  val foreachTableSink = "radio"
  val StreamProviderTableSink = "radioOtherSink"
  val kafkaMetadata = "kafkametadata"

  def getTestInfo() = {
    val rdd = spark.sparkContext.cassandraTable("test", "kv")
    println(rdd.count)
    println(rdd.first)
    println(rdd.map(_.getInt("value")).sum)
  }


  /**
    * remove kafka metadata and only focus on business structure
    */
  private def getDatasetForCassandra(df: DataFrame) = {
    df.select(KafkaService.radioStructureName + ".*")
      .as[SimpleSongAggregation]
  }

  //https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach
  //https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes
  def saveForeach(df: DataFrame) = {
    val ds = CassandraDriver.getDatasetForCassandra(df)

    ds
      .writeStream
      .queryName("KafkaToCassandraForeach")
      //.outputMode("update")
      .foreach(new CassandraSinkForeach())
      .start()
  }

  def saveStreamSinkProvider(df: DataFrame) = {
    df
      .writeStream
      .format("cassandra.sink.CassandraSinkProvider")
      .queryName("KafkaToCassandraStreamSinkProvider")
      //.outputMode("update") //@TODO check how to handle this in a custom StreakSnkProvider
      .start()
  }

  /**
    * @TODO how to retrieve data from cassandra synchronously
    * @TODO handle more topic name, for our example we only use the topic "test"
    *
    *  we can use collect here as kafkameta data is not big at all
    *
    * if no metadata are found, we would use the earliest offsets.
    *
    * @see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-batch
    *  assign	json string {"topicA":[0,1],"topicB":[2,4]}
    *  Specific TopicPartitions to consume. Only one of "assign", "subscribe" or "subscribePattern" options can be specified for Kafka source.
    */
  def getKafaMetadata() = {
    val kafkaMetadataRDD = spark.sparkContext.cassandraTable(namespace, kafkaMetadata)

    if(kafkaMetadataRDD.isEmpty) {
      ("startingOffsets", "earliest")
    } else {
      ("assign", transformKafkaMetadataArrayToJson( kafkaMetadataRDD.collect() ) )
    }
  }

  /**
    * @param array
    * @return {"test":[0,1]}
    */
  def transformKafkaMetadataArrayToJson(array: Array[CassandraRow]) : String = {
      s""""
         {"${KafkaService.topicName}":
          [
           ${array(0).getLong("partition")}, ${array(0).getLong("offset")}
          ]
         }
      """
  }

  def debug() = {
   val output = spark.sparkContext.cassandraTable(namespace, foreachTableSink)

    println(output.count)
    /*  output
     .select("radio, artist, title, count")
     .take(10).foreach(println)*/
  }
}
