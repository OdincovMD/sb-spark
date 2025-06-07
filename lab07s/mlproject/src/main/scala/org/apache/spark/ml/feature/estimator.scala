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
import org.apache.spark.rdd.RDD

// Класс для тренировки
class SklearnEstimator(override val uid: String)
  extends Estimator[SklearnEstimatorModel] with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("SklearnEstimator"))

  override def fit(dataset: Dataset[_]): SklearnEstimatorModel = {
    val spark = dataset.sparkSession

    //  Преобразуем фичи в запись типа features=$domains\tlabel=$label
    val rdd = dataset.select("domains", "gender_age").rdd.map { row =>
      val domains = row.getAs[Seq[String]]("domains").mkString(" ")
      val label = row.getAs[String]("gender_age")
      s"features=$domains\tlabel=$label"
    }

    // Вызов train.py. Важно понимать, что фалй находится на экзекьюторах, не на драйвере + явное указание интерпретатора без шебанинга 
    val modelBase64 = rdd.pipe(Seq("/opt/anaconda/envs/bd9/bin/python3", "./train.py")).collect().mkString("")

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

// Класс для теста
class SklearnEstimatorModel(override val uid: String, val model: String)
      extends Model[SklearnEstimatorModel] with MLWritable {
    
      override def transform(dataset: Dataset[_]): DataFrame = {
          val spark = dataset.sparkSession
          
          import spark.implicits._
        
          val path = Files.createTempFile("lab07s", ".model")
          Files.write(path, model.getBytes("UTF-8"))
          spark.sparkContext.addFile(path.toString)
        
          val domainsRDD = dataset.select("domains").rdd.map(_.getAs[Seq[String]]("domains").mkString(" "))
          val domainsWithIndex = domainsRDD.zipWithIndex().map { case (text, idx) => (idx, text) }
        
          if (domainsWithIndex.isEmpty()) {
            dataset.withColumn("prediction", lit(null).cast(StringType))
          } else {
            val inputRDD: RDD[String] = domainsWithIndex.map(_._2)
            val predictedRDD = inputRDD.pipe(Seq("/opt/anaconda/envs/bd9/bin/python3", "./test.py", "./" + path.getFileName.toString))
              
            val predictionsWithIndex = predictedRDD.zipWithIndex().map { case (pred, idx) => (idx, pred) }
        
            val predictionsDF = predictionsWithIndex.toDF("idx", "prediction")
            val rddWithIndex: RDD[Row] = dataset.rdd.zipWithIndex().map {
              case (row: Row, idx: Long) => Row.fromSeq(Seq(idx) ++ row.toSeq)
            }
            
            val newSchema = StructType(
              StructField("idx", LongType, nullable = false) +: dataset.schema.fields
            )
            
            val originalDF = spark.createDataFrame(rddWithIndex, newSchema)
        
            val joined = originalDF
              .join(predictionsDF, "idx")
              .drop("idx")
                    
            joined
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