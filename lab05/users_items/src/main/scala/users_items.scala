import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.hadoop.fs.{FileSystem, Path}

object users_items {

  def normalize(name: String): String =
    name.toLowerCase.replaceAll("[\\s\\-]+", "_")

  def readEvents(spark: SparkSession, path: String, prefix: String): DataFrame = {
      import spark.implicits._
      spark.read.json(path)
        .select($"uid", $"item_id", $"date")
        .withColumn("item_id", concat(lit(prefix + "_"), regexp_replace(lower($"item_id"), "[\\s\\-]+", "_")))
    }

  def pivot(df: DataFrame): DataFrame = {
    df.groupBy("uid")
      .pivot("item_id")
      .agg(count("*"))
      .na.fill(0)
  }

  def latestDate(spark: SparkSession, df: DataFrame): String = {
    import spark.implicits._
    val maxDate = df.agg(max(to_date($"date", "yyyyMMdd"))).as[java.sql.Date].head()
    val localDate = maxDate.toLocalDate
    localDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
  }

  def latestOutputSubdir(spark: SparkSession, outputDir: String): Option[String] = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val base = new Path(outputDir)
    if (!fs.exists(base)) return None

    val subdirs = fs.listStatus(base).filter(_.isDirectory).map(_.getPath.getName)
    subdirs.filter(_.matches("\\d{8}")).sorted.lastOption
  }

  def mergeMatrices(df1: DataFrame, df2: DataFrame): DataFrame = {
      val allCols = (df1.columns ++ df2.columns).distinct
      val filled1 = allCols.foldLeft(df1)((df, col) => 
        if (df.columns.contains(col)) df else df.withColumn(col, lit(0)))
      val filled2 = allCols.foldLeft(df2)((df, col) => 
        if (df.columns.contains(col)) df else df.withColumn(col, lit(0)))
    
      // Создаем выражения для агрегации
      val aggExprs = allCols
        .filter(_ != "uid")
        .map(c => sum(col(c)).alias(c))
      
      filled1.unionByName(filled2)
        .groupBy("uid")
        .agg(aggExprs.head, aggExprs.tail: _*)
    }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.hadoop.fs.defaultFS", "hdfs://spark-master-1.newprolab.com:8020")
      .getOrCreate()
    
    import spark.implicits._

    val updateMode = spark.conf.get("spark.users_items.update", "1").toInt
    val inputDir = spark.conf.get("spark.users_items.input_dir")
    val outputDir = spark.conf.get("spark.users_items.output_dir")

    // Функция для определения типа файловой системы
    def getFsType(path: String): String = {
      if (path.startsWith("hdfs://")) "hdfs"
      else if (path.startsWith("file://")) "local"
      else "hdfs" // по умолчанию считаем HDFS
    }

    // Функция для нормализации путей
    def normalizePath(path: String): String = {
      val fsType = getFsType(path)
      
      if (path.contains("://")) path 
      else {
        if (fsType == "hdfs") s"hdfs://spark-master-1.newprolab.com:8020$path"
        else s"file://$path"
      }
    }

    val normalizedInputDir = normalizePath(inputDir)
    val normalizedOutputDir = normalizePath(outputDir)

    println(s"Using input path: $normalizedInputDir")
    println(s"Using output path: $normalizedOutputDir")

    // Получаем соответствующую файловую систему
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = new Path(normalizedInputDir).getFileSystem(conf)

    val viewPath = s"$normalizedInputDir/view"
    val buyPath = s"$normalizedInputDir/buy"

    // Проверка существования путей
    if (!fs.exists(new Path(viewPath))) {
      throw new RuntimeException(s"Input path does not exist: $viewPath")
    }

    val viewDF = readEvents(spark, viewPath, "view")
    val buyDF = readEvents(spark, buyPath, "buy")
    val allDF = viewDF.union(buyDF)

    val inputPivoted = pivot(allDF)
    val dateStr = latestDate(spark, allDF)
    val finalOutputPath = s"$normalizedOutputDir/$dateStr"

    val resultDF = if (updateMode == 0) {
      inputPivoted
    } else {
      latestOutputSubdir(spark, normalizedOutputDir) match {
        case Some(prevDate) =>
          val prevPath = s"$normalizedOutputDir/$prevDate"
          if (!fs.exists(new Path(prevPath))) {
            throw new RuntimeException(s"Previous output path does not exist: $prevPath")
          }
          val prevDF = spark.read.parquet(prevPath)
          mergeMatrices(prevDF, inputPivoted)
        case None => inputPivoted
      }
    }

    // Записываем результат
    resultDF.write.mode("overwrite").parquet(finalOutputPath)
    println(s"Successfully saved to: $finalOutputPath")
  }
}