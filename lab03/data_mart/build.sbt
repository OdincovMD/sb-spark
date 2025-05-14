version := "1.0"

scalaVersion := "2.12.18"

name := "data_mart"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.3",
  "org.apache.spark" %% "spark-sql" % "3.4.3",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1",
  "org.elasticsearch" %% "elasticsearch-spark-30" % "8.14.2",
  "org.postgresql" % "postgresql" % "42.7.3",
  "joda-time" % "joda-time" % "2.12.7"
)

resolvers += "confluent" at "https://packages.confluent.io/maven/"
