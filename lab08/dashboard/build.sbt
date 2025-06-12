version := "1.0"

scalaVersion := "2.12.18"

name := "dashboard"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.3",
  "org.apache.spark" %% "spark-mllib" % "3.4.3",
  "org.elasticsearch" %% "elasticsearch-spark-30" % "8.9.0"
)

resolvers += "confluent" at "https://packages.confluent.io/maven/"