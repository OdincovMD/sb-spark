version := "1.0"

scalaVersion := "2.12.18"

name := "agg"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.3",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.3"
)

resolvers += "confluent" at "https://packages.confluent.io/maven/"