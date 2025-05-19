import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

object filter {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    
    import spark.implicits._

    val topicName = spark.conf.get("spark.filter.topic_name")
    val offsetConfig = spark.conf.get("spark.filter.offset")
    val outputDirPrefix = spark.conf.get("spark.filter.output_dir_prefix")

    val kafkaBootstrap = "spark-master-1:6667"

    val isFileProtocol = outputDirPrefix.startsWith("file://")
    val fullOutputPath =
      if (isFileProtocol || outputDirPrefix.startsWith("/")) outputDirPrefix
      else s"/user/mihail.odintsov/$outputDirPrefix"

    val startingOffsets = if (offsetConfig == "earliest") "earliest"
                          else s"""{"$topicName":{"0":$offsetConfig}}"""

    val rawDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", topicName)
      .option("startingOffsets", startingOffsets)
      .load()

    val schema = StructType(Seq(
      StructField("event_type", StringType),
      StructField("category", StringType),
      StructField("item_id", StringType),
      StructField("item_price", IntegerType),
      StructField("uid", StringType),
      StructField("timestamp", LongType)
    ))

    val parsedDF = rawDF
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json($"json_str", schema).as("data"))
      .select("data.*")
      .withColumn("date", date_format(($"timestamp" / 1000).cast("timestamp"), "yyyyMMdd"))
      .withColumn("p_date", $"date")

    val viewDF = parsedDF.filter($"event_type" === "view")
    val buyDF = parsedDF.filter($"event_type" === "buy")

    writePartitioned(viewDF, "view", fullOutputPath, spark, isFileProtocol)
    writePartitioned(buyDF, "buy", fullOutputPath, spark, isFileProtocol)
  }

  def writePartitioned(df: DataFrame, subdir: String, baseOutputPath: String, spark: SparkSession, isFile: Boolean): Unit = {
    val outputPath = baseOutputPath.stripSuffix("/") + s"/$subdir"

    try {
      val uri = if (isFile) new URI(outputPath) else new URI("hdfs:///" + outputPath.stripPrefix("/"))
      val path = new Path(uri)
      val fs = FileSystem.get(uri, spark.sparkContext.hadoopConfiguration)
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    } catch {
      case e: Exception =>
        println(s"Warning: failed to cleanup output path $outputPath. Continuing. Exception: ${e.getMessage}")
    }

    df.write
      .mode("overwrite")
      .partitionBy("p_date")
      .json(outputPath)
  }
}
