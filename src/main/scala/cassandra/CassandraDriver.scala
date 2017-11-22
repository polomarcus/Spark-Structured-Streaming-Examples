package cassandra

import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import kafka.KafkaService
import radio.{SimpleSongAggregation, SimpleSongAggregationKafka}
import spark.SparkHelper
import StreamSinkProvider._
import foreachSink._

object CassandraDriver {
  private val spark = SparkHelper.getSparkSession()
  import spark.implicits._

  val connector = CassandraConnector(SparkHelper.getSparkSession().sparkContext.getConf)

  val namespace = "test"
  val foreachTableSink = "radio"
  val StreamProviderTableSink = "radioothersink"
  val kafkaMetadata = "kafkametadata"

  def getTestInfo() = {
    val rdd = spark.sparkContext.cassandraTable(namespace, kafkaMetadata)

    if( !rdd.isEmpty ) {
      println(rdd.count)
      println(rdd.first)
    } else {
      println(s"$namespace, $kafkaMetadata is empty in cassandra")
    }
  }


  /**
    * remove kafka metadata and only focus on business structure
    */
  def getDatasetForCassandra(df: DataFrame) = {
    df.select(KafkaService.radioStructureName + ".*")
      .as[SimpleSongAggregation]
  }

  //https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach
  //https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes
  def saveForeach(df: DataFrame ) = {
    val ds = CassandraDriver.getDatasetForCassandra(df)

    ds
      .writeStream
      .queryName("KafkaToCassandraForeach")
      .outputMode("update")
      .foreach(new CassandraSinkForeach())
      .start()
  }

  def saveStreamSinkProvider(ds: Dataset[SimpleSongAggregationKafka]) = {
    ds
      .toDF() //@TODO see if we can use directly the Dataset object
      .writeStream
      .format("cassandra.StreamSinkProvider.CassandraSinkProvider")
      .outputMode("update")
      .queryName("KafkaToCassandraStreamSinkProvider")
      .start()
  }

  /**
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

    val output = if(kafkaMetadataRDD.isEmpty) {
      ("startingOffsets", "earliest")
    } else {
      ("startingOffsets", transformKafkaMetadataArrayToJson( kafkaMetadataRDD.collect() ) )
    }

    println("getKafkaMetadata " + output.toString)

    output
  }

  /**
    * @param array
    * @return {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}
    */
  def transformKafkaMetadataArrayToJson(array: Array[CassandraRow]) : String = {
      s"""{"${KafkaService.topicName}":
          {
           "${array(0).getLong("partition")}": ${array(0).getLong("offset")}
          }
         }
      """.replaceAll("\n", "").replaceAll(" ", "")
  }

  def debug() = {
   val output = spark.sparkContext.cassandraTable(namespace, foreachTableSink)

    println(output.count)
  }
}
