import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.PipelineModel
import scala.reflect.io.Directory
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.addFile("python/test.py")

    import spark.implicits._ 

    // Аргументы из spark.conf
    val modelPath = spark.conf.get("spark.model.path")
    val inputTopic = spark.conf.get("spark.kafka.input.topic")
    val outputTopic = spark.conf.get("spark.kafka.output.topic")

    val kafkaBootstrapServers = "spark-master-1:6667"

    // Нужно перезатирать папку с чекпоинтами, иначе спарк не будет ничего пересчитывать
    val checkpointPath = "checkpoints/laba07s_test"
    def deleteRecursively(file: java.io.File): Unit = {
      if (file.isDirectory) file.listFiles.foreach(deleteRecursively)
      file.delete()
    }
    val checkpointDir = new java.io.File(checkpointPath)
    if (checkpointDir.exists) deleteRecursively(checkpointDir)
    
    // Загрузка модели
    val model = PipelineModel.load(modelPath)
    
    // Kafka stream
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "latest")
      .load()
    
    val jsonDF = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]
    
    // Схема JSON
    val schema = StructType(Seq(
      StructField("uid", StringType, nullable = true),
      StructField("visits", ArrayType(StructType(Seq(
        StructField("url", StringType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      ))), nullable = true)
    ))
    
    val parsedDF = jsonDF
      .select(from_json($"value", schema).as("data"))
      .select("data.*")

    // Особенность прекитка кастомного класса во время стриминга      
    val query = parsedDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        
        val predictionDF = model.transform(batchDF)
    
        predictionDF.select($"uid", $"prediction".alias("gender_age"))
          .select(to_json(struct($"uid", $"gender_age")).alias("value"))
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaBootstrapServers)
          .option("topic", outputTopic)
          .save()
      }
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", checkpointPath)
      .start()
    
      query.awaitTermination()
    
    // /opt/spark-3.4.3/bin/spark-submit --class test \
    //   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3 \
    //   --conf spark.model.path=hdfs:///user/mihail.odintsov/model/laba07s \
    //   --conf spark.kafka.input.topic=mihail_odintsov \
    //   --conf spark.kafka.output.topic=mihail_odintsov_lab07s_out \
    //   target/scala-2.12//mlproject_sklearn_2.12-1.0.jar

    // /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --topic mihail_odintsov_lab07s_out --zookeeper spark-node-1.newprolab.com:2181 --partitions 1 --replication-factor 1
    // /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh   --bootstrap-server spark-master-1:6667   --topic mihail_odintsov --from-beginning   --timeout-ms 5000
    // /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh   --bootstrap-server spark-master-1:6667   --topic mihail_odintsov_lab07s_out --from-beginning   --timeout-ms 5000
    // /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper spark-node-1.newprolab.com:2181 --delete --topic mihail_odintsov_lab07s_out
    
  }
}