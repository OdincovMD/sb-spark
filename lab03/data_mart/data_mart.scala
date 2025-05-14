import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.net.{URL, URLDecoder}
import scala.util.Try

object Main {
  def main(args: Array[String]): Unit = {

    // Загружаем переменные окружения через кастомный Env
    val dbHost = Env.get("DB_HOST")
    val dbPort = Env.get("DB_PORT")
    val dbUser = Env.get("DB_USER")
    val dbPassword = Env.get("DB_PASSWORD")
    val cassandraHost = Env.get("CASSANDRA_HOST")
    val cassandraPort = Env.get("CASSANDRA_PORT")
    val esHost = Env.get("ES_HOST")
    val esPort = Env.get("ES_PORT")

    val spark = SparkSession.builder()
      .appName("lab03")
      .config("spark.cassandra.connection.host", cassandraHost)
      .config("spark.cassandra.connection.port", cassandraPort)
      .config("spark.elasticsearch.nodes", esHost)
      .config("spark.elasticsearch.port", esPort)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // cassandra read
    val clients = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load()

    // elasticsearch read
    val visits = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.read.metadata" -> "true",
        "es.nodes" -> esHost,
        "es.port" -> esPort,
        "es.net.ssl" -> "false"))
      .load("visits")

    // hdfs read
    val weblogs = spark.read
      .json("hdfs:///labs/laba03/weblogs.json")

    // postgresql read
    val domainCats = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$dbHost:$dbPort/labdata")
      .option("dbtable", "domain_cats")
      .option("user", dbUser)
      .option("password", dbPassword)
      .option("driver", "org.postgresql.Driver")
      .load()
    
    // age_cat – категория возраста, одна из пяти: 18-24, 25-34, 35-44, 45-54, >=55.
    val clientsWithAgeCat = clients.withColumn("age_cat",
      when(col("age").between(18, 24), "18-24")
      .when(col("age").between(25, 34), "25-34")
      .when(col("age").between(35, 44), "35-44")
      .when(col("age").between(45, 54), "45-54")
      .otherwise(">=55")
    )

    // uid - либо null, если в базе пользователей нет информации об этих посетителях магазина.
    // Названия категорий приводятся к нижнему регистру, пробелы или тире заменяются на подчеркивание.
    // Группируем данные по uid и category, чтобы подсчитать, сколько раз каждый пользователь просматривал товары в каждой категории.
    val shopCategoryCounts = visits
      .filter(col("uid").isNotNull)
      .withColumn("category", lower(regexp_replace(col("category"), "[\\s-]+", "_")))
      .groupBy("uid", "category")
      .agg(count("*").alias("shop_count"))

    // к категории прибавляется приставка shop_.
    val shopPivot = shopCategoryCounts
      .withColumn("category", concat(lit("shop_"), col("category")))
      .groupBy("uid")
      .pivot("category")
      .sum("shop_count")

    // uid - либо null, если в базе пользователей нет информации об этих посетителях магазина.
    // visits: array<struct<timestamp:bigint,url:string>>
    val explodedLogs = weblogs
      .filter(col("uid").isNotNull)
      .withColumn("visit", explode(col("visits")))
      .select(col("uid"), col("visit.url").alias("url"))
    
    // Преобразование URL в домен
    val extractDomain = udf((url: String) => {
        Try {
          val host = new URL(URLDecoder.decode(url, "UTF-8")).getHost
          host.replaceAll("^www\\.", "")
        }.getOrElse("")
      })

    val logsWithDomain = explodedLogs
      .withColumn("domain", extractDomain(col("url")))
      .filter(length(col("domain")) > 0)
    
    val logsWithCat = logsWithDomain.join(domainCats, Seq("domain"))

    val webCategoryCounts = logsWithCat
      .withColumn("category", lower(regexp_replace(col("category"), "[\\s-]+", "_")))
      .groupBy("uid", "category")
      .agg(count("*").alias("web_count"))

    val webPivot = webCategoryCounts
      .withColumn("category", concat(lit("web_"), col("category")))
      .groupBy("uid")
      .pivot("category")
      .sum("web_count")

    val joined = clientsWithAgeCat
      .select("uid", "gender", "age_cat")
      .join(shopPivot, Seq("uid"), "left")
      .join(webPivot, Seq("uid"), "left")

    // Запись в PostgreSQL
    shopPivot.write
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$dbHost:$dbPort/labdata")
      .option("dbtable", "clients")
      .option("user", dbUser)
      .option("password", dbPassword)
      .option("driver", "org.postgresql.Driver")
      .mode("overwrite")
      .save()

    spark.stop()
  }
}
