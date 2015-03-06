name := "TwSpark"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming_2.10" % "1.2.1",
  "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.2.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-alpha2",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2"
)