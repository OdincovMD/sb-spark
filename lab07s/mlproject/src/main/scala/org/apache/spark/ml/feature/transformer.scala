package org.apache.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import java.net.URI

class Url2DomainTransformer(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("Url2DomainTransformer"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    
    val spark = dataset.sparkSession
    import spark.implicits._

    val extractDomain = udf { (url: String) =>
      try {
        val uri = new URI(url)
        val host = uri.getHost
        if (host == null) "" else host.replaceFirst("^www\\.", "")
      } catch {
        case _: Exception => ""
      }
    }

    val withVisit = dataset.withColumn("visit", explode($"visits"))
    val withDomain = withVisit.withColumn("domain", extractDomain($"visit.url"))
    
    // Для инференса не группируем по gender_age
    val result = if (dataset.columns.contains("gender_age")) {
      withDomain
        .groupBy($"uid", $"gender_age")
        .agg(collect_list($"domain").alias("domains"))
        .filter(size($"domains") > 0)
    } else {
      withDomain
        .groupBy($"uid")
        .agg(collect_list($"domain").alias("domains"))
        .filter(size($"domains") > 0)
    }
    result
  }

  override def copy(extra: ParamMap): Url2DomainTransformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    require(schema.fieldNames.contains("uid"), "Dataset must contain 'uid'")
    require(schema.fieldNames.contains("visits"), "Dataset must contain 'visits'")
    
    val newSchema = schema.add("domains", ArrayType(StringType, containsNull = true), nullable = true)
    newSchema
  }
}

object Url2DomainTransformer extends DefaultParamsReadable[Url2DomainTransformer] {
  override def load(path: String): Url2DomainTransformer = super.load(path)
}
