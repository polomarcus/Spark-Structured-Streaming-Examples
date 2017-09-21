resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

name := "SparkStructuredStreamingCassandra"

version := "1.0"
scalaVersion := "2.11.7"
val sparkVersion = "2.2.0"

libraryDependencies += "log4j" % "log4j" % "1.2.14"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2",
  "com.couchbase.client" %% "spark-connector" % sparkVersion
)
