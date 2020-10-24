import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}

object test  extends App {

  val sparkSession = SparkSession
    .builder()
    .appName("lab07_33")
    .getOrCreate()

  import sparkSession.implicits._

  val model = PipelineModel.load("/user/tatiana.dvoryaninova/model_test1")

  val schemaJson = StructType(
    List(StructField("uid", StringType), StructField("domains", ArrayType(StructType(
      List(StructField("url", StringType), StructField("timestamp", StringType)))))))

  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "tatiana_dvoryaninova",
    "startingOffsets" -> """earliest"""
  )
  val dfKafka = sparkSession.readStream.format("kafka").options(kafkaParams).load

  val dfClearJson = dfKafka
    .select(from_json($"value".cast("String"), schemaJson).as("parsed_json"))
    .select ($"parsed_json.*")
    .select('uid, explode('domains).alias("visits"))
    .withColumn("url", col("visits").getField("url"))
    .drop("visits")
    .groupBy("uid").agg(collect_list('url).alias("domains"))
    .select('uid, 'domains)

  val df = model.transform(dfClearJson)
    .select('uid, 'predictedLabel.alias("gender_age"))

  val dfKafkaOutput = df
    .selectExpr("CAST(uid AS STRING) AS key", "to_json(struct(*)) as value")
    .writeStream
    .outputMode("update")
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("topic", "tatiana_dvoryaninova_lab04b")
    .start()

  dfKafkaOutput.awaitTermination()
}
