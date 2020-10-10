import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{udf, col}

object users_items  extends App {

  override def main(args: Array[String]) = {
    val sparkSession = SparkSession
      .builder()
      .appName("lab05_33")
      .getOrCreate()
    import sparkSession.implicits._

    val update = if (sparkSession.conf.get("spark.users_items.update") == 0) 0 else 1
    val output_dir = sparkSession.conf.get("spark.users_items.output_dir")
    val input_dir = sparkSession.conf.get("spark.users_items.input_dir")

    val time = sparkSession.read.json(input_dir + "/view/*")
      .union(sparkSession.read.json(input_dir + "/buy/*"))
      .where('uid.isNotNull)
      .agg(max('timestamp))
      .withColumn("date", to_date(to_utc_timestamp(($"max(timestamp)"/1000).cast("timestamp"), "Europe/Moscow"),"YYYYMMDD"))
      .select('date)
      .collect
    val fileName = time(0).get(0).toString.replace("-","")

    val temp = sparkSession.read.json(input_dir + "/view/*")
      .union(sparkSession.read.json(input_dir + "/buy/*"))
      .where('uid.isNotNull)
      .withColumn("item", concat('event_type, lit("_"), lower(regexp_replace('item_id, "[\\s-]+", "_"))))
      .select('uid, 'item)
      .groupBy('uid)
      .pivot('item)
      .agg(count('uid))
      .na.fill(0)

    if(update == 0) {
      val oldRes = sparkSession
        .read
        .format("parquet")
        .load(output_dir + "/*")
    }

    temp.write
      .mode("overwrite")
      .parquet(output_dir + "/" + fileName)
  }
}
