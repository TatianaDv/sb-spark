import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{udf, col}

object filter  extends App {

  override def main(args: Array[String]) = {
    val sparkSession = SparkSession
      .builder()
      .appName("lab04_33")
      .getOrCreate()
    import sparkSession.implicits._

    val offset = sparkSession.conf.get("spark.filter.offset")
    val subscr = sparkSession.conf.get("spark.filter.topic_name")
    val df = sparkSession
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", "spark-master-1:6667")
        .option("subscribe", subscr)
        .option("failOnDataLoss", false)
        .option("startingOffsets",if (offset == "earliest") s"$offset" else s""" { "$subscr": {"0": $offset }} """)
        .load()

    val jsonString = df
        .select(col("value")
        .cast("string")).as[String]
    val parsed = sparkSession
        .read
        .json(jsonString)
    var withColDate = parsed
        .select('category,'event_type,'item_id,'item_price,'timestamp,'uid)
        //.withColumn("date", date_format((from_unixtime(($"timestamp"/1000))).cast("date"), "YYYYMMDD"))
      .withColumn("date", to_date(($"timestamp"/1000).cast("timestamp"),"YYYYMMDD"))
        .withColumn("p_date", col("date"))

    val output_dir_prefix = sparkSession.conf.get("spark.filter.output_dir_prefix")
    withColDate.show(5)
    println(output_dir_prefix)
    withColDate
      .filter('event_type === "view")
      .write
      .format("json")
      .mode("overwrite")
      .partitionBy("p_date")
      .save(output_dir_prefix + "/view/")

    withColDate
      .filter('event_type === "buy")
      .write
      .format("json")
      .mode("overwrite")
      .partitionBy("p_date")
      .save(output_dir_prefix + "/buy/")
  }
}