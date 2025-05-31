version := "1.0"

scalaVersion := "2.12.18"

name := "features"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.3" % "provided",
  "org.apache.spark" %% "spark-core" % "3.4.3"
)

resolvers += "confluent" at "https://packages.confluent.io/maven/"

