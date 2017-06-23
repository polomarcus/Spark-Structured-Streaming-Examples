resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

name := "SparkStructuredStreamingCassandra"

version := "1.0"
scalaVersion := "2.11.7"
val sparkVersion = "2.1.1"

libraryDependencies += "log4j" % "log4j" % "1.2.14"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % sparkVersion
libraryDependencies += "net.ruippeixotog" %% "scala-scraper" % "1.2.0"
libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2"
