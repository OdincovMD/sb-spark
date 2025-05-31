import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession 


object features {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    import spark.implicits._

    val weblogsDF = spark.read.json("/labs/laba03/")
    val withDomains = weblogsDF
      .withColumn("visit", explode($"visits"))
      .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .filter($"domain".isNotNull)

    val topDomains = withDomains
      .groupBy("domain")
      .count()
      .orderBy(desc("count"))
      .limit(1000)
      .select("domain")
      .as[String]
      .collect()
      .sorted

    val broadcastTop = spark.sparkContext.broadcast(topDomains)

    val filtered = withDomains
      .filter(col("domain").isin(topDomains: _*))
      .groupBy("uid", "domain")
      .agg(count("*").alias("domain_count"))

    val pivotDF = filtered
      .groupBy("uid")
      .pivot("domain", topDomains)
      .agg(first("domain_count"))
      .na.fill(0)

    val domainFeatures = pivotDF
      .withColumn(
        "domain_features",
        array(topDomains.map(d => col(s"`$d`").cast("int")): _*)
      )
      .select("uid", "domain_features")

    val timestampsDF = weblogsDF
      .withColumn("visit", explode($"visits"))
      .withColumn("timestamp", $"visit.timestamp")
      .select($"uid", $"timestamp")
      .filter($"timestamp".isNotNull)
    
    val withDay = timestampsDF.withColumn(
      "day_of_week",
      date_format(from_unixtime($"timestamp" / 1000), "E").alias("dow")
    )

    val daysMapping = Map(
      "Mon" -> "web_day_mon", "Tue" -> "web_day_tue", "Wed" -> "web_day_wed",
      "Thu" -> "web_day_thu", "Fri" -> "web_day_fri", "Sat" -> "web_day_sat",
      "Sun" -> "web_day_sun"
    )

    val dayCounts = withDay
      .groupBy("uid", "day_of_week")
      .agg(count("*").alias("cnt"))
      .filter($"day_of_week".isin(daysMapping.keys.toSeq: _*))
      .withColumn("day_of_week", expr(s"CASE ${daysMapping.map { case (k, v) => s"WHEN day_of_week = '$k' THEN '$v'" }.mkString(" ")} END"))
      .groupBy("uid")
      .pivot("day_of_week", daysMapping.values.toSeq)
      .agg(first("cnt"))
      .na.fill(0)
    
    val withHour = timestampsDF.withColumn(
      "hour_of_day",
      hour(from_unixtime($"timestamp" / 1000))
    )

    val hourCounts = withHour
      .groupBy("uid", "hour_of_day")
      .agg(count("*").alias("cnt"))
      .groupBy("uid")
      .pivot("hour_of_day", 0 to 23)
      .agg(first("cnt"))
      .na.fill(0)
      .toDF(Seq("uid") ++ (0 to 23).map(h => s"web_hour_$h"): _*)
    
    val withHourFlag = withHour.withColumn(
      "is_work",
      when($"hour_of_day".between(9, 17), 1).otherwise(0)
    ).withColumn(
      "is_evening",
      when($"hour_of_day".between(18, 23), 1).otherwise(0)
    )

    val timeFractions = withHourFlag.groupBy("uid").agg(
      sum("is_work").alias("work_count"),
      sum("is_evening").alias("evening_count"),
      count("*").alias("total_count")
    ).withColumn(
      "web_fraction_work_hours", $"work_count" / $"total_count"
    ).withColumn(
      "web_fraction_evening_hours", $"evening_count" / $"total_count"
    ).select("uid", "web_fraction_work_hours", "web_fraction_evening_hours")
    
    val webFeatures = domainFeatures
      .join(dayCounts, Seq("uid"), "left")
      .join(hourCounts, Seq("uid"), "left")
      .join(timeFractions, Seq("uid"), "left")
      .na.fill(0)
    
    val usersItemsDF = spark.read.parquet("/user/mihail.odintsov/users-items/20200429")
    val fullFeatures = usersItemsDF.join(webFeatures, Seq("uid"), "left").na.fill(0)

    fullFeatures.write.mode("overwrite").parquet("/user/mihail.odintsov/features")
  }
}