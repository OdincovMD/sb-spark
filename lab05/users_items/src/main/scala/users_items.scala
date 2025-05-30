import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

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
    val inputDirRaw = spark.conf.get("spark.users_items.input_dir")
    val outputDirRaw = spark.conf.get("spark.users_items.output_dir")
 
    def isFileProtocol(path: String): Boolean = path.startsWith("file://") || path.startsWith("/")

    // Определим абсолютные пути
    val inputPathPrefix =
      if (isFileProtocol(inputDirRaw)) inputDirRaw.stripSuffix("/")
      else s"hdfs://spark-master-1.newprolab.com:8020/$inputDirRaw".stripSuffix("/")

    val outputPathPrefix =
      if (isFileProtocol(outputDirRaw)) outputDirRaw.stripSuffix("/")
      else s"hdfs://spark-master-1.newprolab.com:8020/$outputDirRaw".stripSuffix("/")

    println(s"Using input path: $inputPathPrefix")
    println(s"Using output path: $outputPathPrefix")

    val viewPath = s"$inputPathPrefix/view"
    val buyPath = s"$inputPathPrefix/buy"

    // Получаем FS для input-а
    val inputUri = new URI(viewPath)
    val inputConf = spark.sparkContext.hadoopConfiguration
    // Для локальных файлов используем RawLocalFileSystem
    if (isFileProtocol(viewPath)) {
      inputConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
      inputConf.set("fs.defaultFS", "file:///")
    }
    val inputFs = FileSystem.get(inputUri, inputConf)
 
    if (!inputFs.exists(new Path(viewPath))) {
      throw new RuntimeException(s"Input path does not exist: $viewPath")
    }

    val viewDF = readEvents(spark, viewPath, "view")
    val buyDF = readEvents(spark, buyPath, "buy")
    val allDF = viewDF.union(buyDF)

    val inputPivoted = pivot(allDF)
    val dateStr = latestDate(spark, allDF)
    val fullOutputPath = s"$outputPathPrefix/$dateStr"

    // Очистим директорию, если нужно
    try {
      val outputUri = new URI(fullOutputPath)
      val outputConf = spark.sparkContext.hadoopConfiguration
      // Для локальных файлов используем RawLocalFileSystem
      if (isFileProtocol(fullOutputPath)) {
        outputConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
        outputConf.set("fs.defaultFS", "file:///")
      }
      val outputFs = FileSystem.get(outputUri, outputConf)
      val outputPath = new Path(outputUri)

      if (outputFs.exists(outputPath)) {
        outputFs.delete(outputPath, true)
      }
    } catch {
      case e: Exception =>
      println(s"Warning: failed to cleanup output path $fullOutputPath. Continuing. Exception: ${e.getMessage}")
    }
    
    val resultDF = if (updateMode == 0) {
      inputPivoted
    } else {
      latestOutputSubdir(spark, outputPathPrefix) match {
        case Some(prevDate) =>
          val prevPath = s"$outputPathPrefix/$prevDate"
          val prevUri = new URI(prevPath)
          val prevConf = spark.sparkContext.hadoopConfiguration
          if (isFileProtocol(prevPath)) {
            prevConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
            prevConf.set("fs.defaultFS", "file:///")
          }
          val prevFs = FileSystem.get(prevUri, prevConf)
        
          if (!prevFs.exists(new Path(prevUri))) {
            throw new RuntimeException(s"Previous output path does not exist: $prevPath")
          }

          val prevDF = spark.read.parquet(prevPath)
          mergeMatrices(prevDF, inputPivoted)
        case None => inputPivoted
      }
    }
    // Сохраняем результат
    resultDF.write.mode("overwrite").parquet(fullOutputPath)
    println(s"Successfully saved to: $fullOutputPath")
  }
}
