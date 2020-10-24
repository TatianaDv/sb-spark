import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}

object test  extends App {

  val sparkSession = SparkSession
    .builder()
    .appName("lab07_33")
    .getOrCreate()

  var modelDir = sparkSession.conf.get("spark.mlproject.modeldir")
  var topicInput = sparkSession.conf.get("spark.mlproject.topicinput")
  var topicOutput = sparkSession.conf.get("spark.mlproject.topicoutput")

  import sparkSession.implicits._

  val model = PipelineModel.load(modelDir)

  val schemaJson = StructType(
    Seq(StructField("uid", StringType), StructField("visits", ArrayType(StructType(
      Seq(StructField("url", StringType), StructField("timestamp", LongType)))))))

  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> topicInput,
    "maxOffsetsPerTrigger"    -> "5000",
    "startingOffsets" -> "earliest"
  )
  val dfKafka = sparkSession.readStream.format("kafka").options(kafkaParams).load

  val dfClearJson = dfKafka
    .select(from_json($"value".cast("String"), schemaJson).as("parsed_json"))
    .select ($"parsed_json.*")
    .select('uid, explode('visits).alias("visits"))
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
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("topic", topicOutput)
    .option("checkpointLocation", "chk_tatiana_dvoryaninova_t4")
    .start()

  dfKafkaOutput.awaitTermination()
}
