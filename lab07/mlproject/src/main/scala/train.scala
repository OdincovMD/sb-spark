import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}
import java.net.URI

object train {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    // Считаем аргументы из spark.conf
    val trainPath = spark.conf.get("spark.train.data.path")
    val modelPath = spark.conf.get("spark.model.save.path")

    // UDF для извлечения домена из url
    val extractDomain = udf { (url: String) =>
      try {
        val uri = new URI(url)
        val host = uri.getHost
        if (host == null) "" else host.replaceFirst("^www\\.", "")
      } catch {
        case _: Exception => ""
      }
    }

    val rawDF = spark.read.json(trainPath)

    // explode visits -> extract domain -> collect domains per uid
    val exploded = rawDF
      .withColumn("visit", explode($"visits"))
      .withColumn("domain", extractDomain($"visit.url"))
      .groupBy("uid", "gender_age")
      .agg(collect_list($"domain").alias("domains"))
      .filter(size($"domains") > 0) // отфильтровать пустые

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val indexerModel = indexer.fit(exploded)

    val indexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predicted_gender_age")
      .setLabels(indexerModel.labels)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexerModel, lr, indexToString))

    val model = pipeline.fit(exploded)

    model.write.overwrite().save(modelPath)
  }
}
