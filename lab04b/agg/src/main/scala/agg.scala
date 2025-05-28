import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.reflect.io.Directory

object agg extends App {

  val checkpointPath = "checkpoint"
  val checkpointDir = new Directory(new java.io.File(checkpointPath))
  if (checkpointDir.exists) checkpointDir.deleteRecursively()

  val spark = SparkSession.builder().getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val schema = new StructType()
    .add("event_type", StringType)
    .add("category", StringType)
    .add("item_id", StringType)
    .add("item_price", LongType)
    .add("uid", StringType)
    .add("timestamp", LongType)

  val kafkaInput = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", "mihail_odintsov")
    .option("startingOffsets", "earliest")
    .load()

  val parsed = kafkaInput
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json($"json_str", schema).as("data"))
    .selectExpr(
      "data.event_type",
      "data.category",
      "data.item_id",
      "data.item_price",
      "data.uid",
      "data.timestamp",
      "(data.timestamp / 1000) as event_ts",
      "CAST(data.timestamp / 1000 AS TIMESTAMP) as event_time"
    )

  val aggregated = parsed
    .withWatermark("event_time", "1 hour")
    .groupBy(window($"event_time", "1 hour"))
    .agg(
      sum(when($"event_type" === "buy", $"item_price")).alias("revenue"),
      approx_count_distinct(when($"uid".isNotNull, $"uid")).alias("visitors"),
      count(when($"event_type" === "buy", 1)).alias("purchases")
    )
    .select(
      $"window.start".alias("start_ts"),
      $"revenue",
      $"visitors",
      $"purchases"
    )

  val result = aggregated.selectExpr("to_json(struct(*)) as value")

  val query = result.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("topic", "mihail_odintsov_lab04b_out")
    .option("checkpointLocation", checkpointPath)
    .outputMode("update")
    .start()

  query.awaitTermination()
}
