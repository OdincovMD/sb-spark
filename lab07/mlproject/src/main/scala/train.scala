import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
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

    // Векторизация признаков: вектор частот — то есть ["vk.com", "vk.com", "google.com"] → [vk:2, google:1, ...]
    val cvModel = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")
    
    // Индексирование меток: F:18-24 → 0, M:25-34 → 1
    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
      .fit(exploded)
    
    // RandomForest
    val rf = new RandomForestClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setSeed(42)
    
    // Конвертация предсказания обратно в строку
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predicted_gender_age")
      .setLabels(indexer.labels)
    
    // Пайплайн
    val pipeline = new Pipeline()
      .setStages(Array(cvModel, indexer, rf, labelConverter))
    
    // Грид гиперпараметров
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(20, 50))
      .addGrid(rf.maxDepth, Array(5, 10))
      .addGrid(cvModel.minDF, Array(1.0, 5.0))
      .build()
    
    // Оценка точности
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
      
    // 3-кратная кросс-валидацию
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
    
    // Обучение
    val model = cv.fit(exploded)

    // Приведение к PipelineModel и сохранение
    val bestPipelineModel = model.bestModel.asInstanceOf[PipelineModel]
    bestPipelineModel.write.overwrite().save(modelPath)
  }
}
