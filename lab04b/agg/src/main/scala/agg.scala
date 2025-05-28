import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object agg extends App {

  val spark = SparkSession.builder().getOrCreate()

  import spark.implicits._

  val kafkaBootstrapServers = "spark-master-1:6667"
  val inputTopic = "mihail_odintsov"
  val outputTopic = "mihail_odintsov_lab04b_out"

  // Определяем схему входных сообщений
  val schema = StructType(Seq(
    StructField("event_type", StringType, nullable = true),
    StructField("category", StringType, nullable = true),
    StructField("item_id", StringType, nullable = true),
    StructField("item_price", IntegerType, nullable = true),
    StructField("uid", StringType, nullable = true),
    StructField("timestamp", LongType, nullable = true)
  ))

  // Чтение из Kafka
  val kafkaRaw = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
    .option("subscribe", inputTopic)
    .option("startingOffsets", "earliest")
    .load()

  // Преобразуем значение value -> JSON -> колонки
  val parsed = kafkaRaw
    .selectExpr("CAST(value AS STRING)")
    .select(from_json($"value", schema).as("data"))
    .select("data.*")
    .withColumn("event_time", ($"timestamp" / 1000).cast("timestamp"))

  // Агрегация по 1-часовому окну
  val aggregated = parsed
    .withWatermark("event_time", "1 hour")
    .groupBy(window($"event_time", "1 hour"))
    .agg(
      sum(when($"event_type" === "buy", $"item_price")).as("revenue"),
      approx_count_distinct(when($"uid".isNotNull, $"uid")).as("visitors"),
      count(when($"event_type" === "buy", lit(1))).as("purchases")
    )
    .withColumn("aov", $"revenue" / $"purchases")
    .withColumn("start_ts", unix_timestamp($"window.start"))
    .withColumn("end_ts", unix_timestamp($"window.end"))
    .select("start_ts", "end_ts", "revenue", "visitors", "purchases", "aov")
    .select(to_json(struct($"*")).alias("value"))

  // Запись в Kafka
  val query = aggregated.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
    .option("topic", outputTopic)
    .option("checkpointLocation", "/user/mihail.odintsov/checkpoints/lab04b")
    .outputMode("update")
    .start()

  query.awaitTermination()
}
