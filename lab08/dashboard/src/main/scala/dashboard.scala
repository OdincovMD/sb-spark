import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types._
import java.net.URI
import scala.reflect.io.Directory
import org.apache.spark.sql.streaming.Trigger

object dashboard {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("es.nodes", "10.0.0.31")
      .config("es.port", "9200")
      .getOrCreate()

    import spark.implicits._
    
    val modelPath = spark.conf.get("spark.model.path")
    val inputPath = spark.conf.get("spark.input.path", "/labs/laba08")
    val elasticIndex = spark.conf.get("spark.elastic.index", "mihail_odintsov_lab08")

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

    val schema = StructType(Seq(
      StructField("uid", StringType, nullable = true),
      StructField("date", LongType, nullable = true),
      StructField("visits", ArrayType(StructType(Seq(
        StructField("url", StringType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      ))), nullable = true)
    ))

    val parsedDF = spark.read
      .schema(schema)
      .json(inputPath)

    val exploded = parsedDF
      .withColumn("visit", explode($"visits"))
      .withColumn("domain", extractDomain($"visit.url"))
      .groupBy("uid", "date")
      .agg(collect_list($"domain").alias("domains"))
      .filter(size($"domains") > 0)
    
    val predictions = model.transform(exploded)

    val esDF = predictions.select(
      $"uid",
      $"predicted_gender_age".alias("gender_age"),
      $"date" 
    )

    esDF.write
      .format("org.elasticsearch.spark.sql")
      .option("es.resource", s"${elasticIndex}/_doc")
      .option("es.mapping.id", "uid")
      .mode("append")
      .save()
  }
}