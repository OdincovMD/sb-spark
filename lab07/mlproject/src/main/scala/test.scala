import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import java.net.URI

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    // Аргументы из spark.conf
    val modelPath = spark.conf.get("spark.model.path")
    val inputTopic = spark.conf.get("spark.kafka.input.topic")
    val outputTopic = spark.conf.get("spark.kafka.output.topic")

    val kafkaBootstrapServers = "spark-master-1:6667"

    val checkpointPath = "checkpoints/laba07_test"
    val checkpointDir = new Directory(new java.io.File(checkpointPath))
    if (checkpointDir.exists) checkpointDir.deleteRecursively()

    val extractDomain = udf { (url: String) =>
      try {
        val uri = new URI(url)
        val host = uri.getHost
        if (host == null) "" else host.replaceFirst("^www\\.", "")
      } catch {
        case _: Exception => ""
      }
    }

    val model = PipelineModel.load(modelPath)

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "latest")
      .load()

    val jsonDF = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    val parsedDF = jsonDF.select(from_json($"value",
      """
        uid STRING,
        visits ARRAY<STRUCT<url: STRING, timestamp: LONG>>
      """
    ).as("data"))
      .select("data.*")

    val exploded = parsedDF
      .withColumn("visit", explode($"visits"))
      .withColumn("domain", extractDomain($"visit.url"))
      .groupBy("uid")
      .agg(collect_list($"domain").alias("domains"))
      .filter(size($"domains") > 0)

    val predictions = model.transform(exploded)

    val output = predictions.select($"uid", $"predicted_gender_age".alias("gender_age"))
      .select(to_json(struct($"uid", $"gender_age")).alias("value"))

    val query = output.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", outputTopic)
      .option("checkpointLocation", checkpointPath)
      .start()

    query.awaitTermination()
  }
}
