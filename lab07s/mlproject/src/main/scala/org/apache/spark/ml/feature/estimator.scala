package org.apache.spark.ml.feature

import org.apache.spark.ml._
import org.apache.spark.ml.util._
import org.apache.spark.ml.param._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path
import java.io._
import java.nio.file.{Files, Paths}
import org.apache.spark.SparkFiles
import scala.sys.process._

class SklearnEstimator(override val uid: String)
  extends Estimator[SklearnEstimatorModel] with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("SklearnEstimator"))

  override def fit(dataset: Dataset[_]): SklearnEstimatorModel = {
    val spark = dataset.sparkSession

    val rdd = dataset.select("domains", "gender_age").rdd.map { row =>
      val domains = row.getAs[Seq[String]]("domains").mkString(" ")
      val label = row.getAs[String]("gender_age")
      s"features=$domains\tlabel=$label"
    }

    val modelBase64 = rdd.pipe("train.py").collect().mkString("")
    new SklearnEstimatorModel(uid, modelBase64)
  }

  override def copy(extra: ParamMap): SklearnEstimator = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add(StructField("prediction", StringType, nullable = false))
  }
}

object SklearnEstimator extends DefaultParamsReadable[SklearnEstimator] {
  override def load(path: String): SklearnEstimator = super.load(path)
}

class SklearnEstimatorModel(override val uid: String, val model: String)
  extends Model[SklearnEstimatorModel] with MLWritable {

  override def transform(dataset: Dataset[_]): DataFrame = {
    val spark = dataset.sparkSession
    val path = Files.createTempFile("lab07s", ".model")
    try {
      Files.write(path, model.getBytes("UTF-8"))
      spark.sparkContext.addFile(path.toString)

      val rdd = dataset.select("domains").rdd.map { row =>
        row.getAs[Seq[String]]("domains").mkString(" ")
      }

      val predictions = rdd.pipe("test.py").collect()

      import spark.implicits._
      val predsDF = predictions.toSeq.toDF("prediction")
        .withColumn("row_id", monotonically_increasing_id())

      dataset.withColumn("row_id", monotonically_increasing_id())
        .join(predsDF, "row_id")
        .drop("row_id")
    } finally {
      Files.deleteIfExists(path)
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add(StructField("prediction", StringType, nullable = false))
  }

  override def copy(extra: ParamMap): SklearnEstimatorModel = defaultCopy(extra)

  override def write: MLWriter = new SklearnEstimatorModel.SklearnEstimatorModelWriter(this)
}

object SklearnEstimatorModel extends MLReadable[SklearnEstimatorModel] {
  private val className = classOf[SklearnEstimatorModel].getName

  private case class Data(model: String)

  private class SklearnEstimatorModelWriter(instance: SklearnEstimatorModel) extends MLWriter {
    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.model)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class SklearnEstimatorModelReader extends MLReader[SklearnEstimatorModel] {
    override def load(path: String): SklearnEstimatorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val row = sparkSession.read.parquet(dataPath).select("model").head()
      val modelStr = row.getAs[String](0)
      val model = new SklearnEstimatorModel(metadata.uid, modelStr)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[SklearnEstimatorModel] = new SklearnEstimatorModelReader
  override def load(path: String): SklearnEstimatorModel = super.load(path)
}