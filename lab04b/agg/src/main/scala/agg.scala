import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.reflect.io.Directory
import org.apache.spark.sql.streaming.Trigger

object agg extends App {
  // Нужно перезатирать папку с чекпоинтами, иначе спарк не будет ничего пересчитывать
  val checkpointPath = "checkpoint"
  val checkpointDir = new Directory(new java.io.File(checkpointPath))
  if (checkpointDir.exists) checkpointDir.deleteRecursively()

  val spark = SparkSession.builder().getOrCreate()
    
  import spark.implicits._

  spark.sparkContext.setLogLevel("INFO")
  // Размер микробатча данных, получаемых из Kafka, определяется временным промежутком, равным 5 секундам.
  spark.conf.set("spark.sql.streaming.microBatchDuration", "5s")

  val schema = StructType(Seq(
    StructField("event_type", StringType),
    StructField("category", StringType),
    StructField("item_id", StringType),
    StructField("item_price", IntegerType),
    StructField("uid", StringType),
    StructField("timestamp", LongType)
  ))

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
    .groupBy(window($"event_time", "1 hour", "1 hour"))
    .agg(
      sum(when($"event_type" === "buy", $"item_price")).alias("revenue"),
      // countDistinct не может в стриминг
      // approx_count_distinct может в стриминг, но не может в точность
      size(collect_set($"uid")).alias("visitors"),
      count(when($"event_type" === "buy", lit(1))).alias("purchases")
    )
    .withColumn("aov", $"revenue" / $"purchases")
    .select(
      unix_timestamp($"window.start").alias("start_ts"),  // Конвертируем в Unix timestamp
      unix_timestamp($"window.end").alias("end_ts"),     // Конвертируем в Unix timestamp
      $"revenue",
      $"visitors",
      $"purchases",
      $"aov"
    )

  val result = aggregated
    .select(to_json(struct($"*")).alias("value"))
  // каждый раз нужно удалять топик
  // /usr/hdp/current/kafka-broker/bin/kafka-topics.sh   --zookeeper spark-node-1.newprolab.com:2181   --delete   --topic mihail_odintsov_lab04b_out
    
  // проверка того, что происходит стриминг
  // /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh   --bootstrap-server spark-master-1:6667   --topic mihail_odintsov_lab04b_out   --from-beginning   --timeout-ms 5000
  // /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh   --bootstrap-server spark-master-1:6667   --topic mihail_odintsov --from-beginning   --timeout-ms 5000
    
  val query = result.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("topic", "mihail_odintsov_lab04b_out")
    .option("checkpointLocation", checkpointPath)
    .outputMode("update")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .start()

  query.awaitTermination()
}