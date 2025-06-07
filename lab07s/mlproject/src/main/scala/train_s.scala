import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer}
import org.apache.spark.ml.feature.Url2DomainTransformer
import org.apache.spark.ml.feature.SklearnEstimator

object train {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.addFile("python/train.py")

    // Считаем аргументы из spark.conf
    val trainPath = spark.conf.get("spark.train.data.path")
    val modelPath = spark.conf.get("spark.model.save.path")

    val rawDF = spark.read.json(trainPath)

    val domainTransformer = new Url2DomainTransformer()

    val cvModel = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val estimator = new SklearnEstimator()

    val pipeline = new Pipeline()
      .setStages(Array(domainTransformer, cvModel, indexer, estimator))

    val model = pipeline.fit(rawDF)

    model.write.overwrite().save(modelPath)
  }
}
