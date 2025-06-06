import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types._
import java.net.URI
import scala.reflect.io.Directory
import org.apache.spark.sql.streaming.Trigger

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    // Аргументы из spark.conf
    val modelPath = spark.conf.get("spark.model.path")
    val inputTopic = spark.conf.get("spark.kafka.input.topic")
    val outputTopic = spark.conf.get("spark.kafka.output.topic")

    val kafkaBootstrapServers = "spark-master-1:6667"

    // Нужно перезатирать папку с чекпоинтами, иначе спарк не будет ничего пересчитывать
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

    // Загрузка модели
    val model = PipelineModel.load(modelPath)

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "latest")
      .load()

    val jsonDF = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    val schema = StructType(Seq(
      StructField("uid", StringType, nullable = true),
      StructField("visits", ArrayType(StructType(Seq(
        StructField("url", StringType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      ))), nullable = true)
    ))

    val parsedDF = jsonDF
      .select(from_json($"value", schema).as("data"))
      .select("data.*")

    val exploded = parsedDF
      .withColumn("visit", explode($"visits"))
      .withColumn("domain", extractDomain($"visit.url"))
      .groupBy("uid")
      .agg(collect_list($"domain").alias("domains"))
      .filter(size($"domains") > 0)
    
    // Инференс модели 
    val predictions = model.transform(exploded)

    val output = predictions.select($"uid", $"predicted_gender_age".alias("gender_age"))
      .select(to_json(struct($"uid", $"gender_age")).alias("value"))

    // /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --topic mihail_odintsov_lab07_out --zookeeper spark-node-1.newprolab.com:2181 --partitions 1 --replication-factor 1
    // /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh   --bootstrap-server spark-master-1:6667   --topic mihail_odintsov --from-beginning   --timeout-ms 5000
    // /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh   --bootstrap-server spark-master-1:6667   --topic mihail_odintsov_lab07_out --from-beginning   --timeout-ms 5000
    // /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper spark-node-1.newprolab.com:2181 --delete --topic mihail_odintsov_lab07_out
    
    val query = output.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", outputTopic)
      .option("checkpointLocation", checkpointPath)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }
}
