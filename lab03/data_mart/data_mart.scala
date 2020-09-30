import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, explode, lit, udf, when}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{when, lit};
import java.net.URLDecoder
import scala.util.Try
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.sql.functions

object data_mart  extends App{

  override def main(args: Array[String]) = {

    val sparkSession = SparkSession
      .builder()
      .appName("lab03_33")
      .getOrCreate()

    import sparkSession.implicits._
    //shop logs
    val esOptions = Map("pushdown" -> "true", "es.nodes" -> "10.0.0.5", "es.port" -> "9200", "es.resource" -> "visits")
    val shopLogsDf = sparkSession.read
      .format("org.elasticsearch.spark.sql")
      .options(esOptions)
      .load("index/visits")
      .withColumn("shop_cat", concat(lit("shop_"), lower(regexp_replace('category, "[\\s-]+", "_"))))
      .select('uid, 'shop_cat)
      .where(col("uid").isNotNull)

    val shopCategoryRes =
      shopLogsDf
        .groupBy('uid)
        .pivot('shop_cat)
        .agg(count('uid))
        .drop('shop_cat)

    // web logs
    sparkSession.conf.set("spark.cassandra.connection.host", "10.0.0.5")
    sparkSession.conf.set("spark.cassandra.connection.port", "9042")
    sparkSession.conf.set("spark.cassandra.output.consistency.level", "ANY")
    sparkSession.conf.set("spark.cassandra.input.consistency.level", "ONE")

    val csOptions = Map("table" -> "clients","keyspace" -> "labdata", "spark.cassandra.connection.host" -> "10.0.0.5")
    val clientsSource = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(csOptions)
      .load

    clientsSource.createOrReplaceTempView("df")
    val clients = sparkSession.sql("select uid, gender, CASE WHEN age >= 18 AND age < 24 THEN '18-24' WHEN age >= 25 AND age < 34 THEN '25-34' WHEN age >= 35 AND age < 44 THEN '35-44' ELSE '>=55' END AS age_cat from df")
    
    val webCatSource = sparkSession.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://10.0.0.5:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "tatiana_dvoryaninova")
      .option("password", "IDke0iOn")
      .load
      .withColumn("web_cat",concat(lit("web_"),'category))

    val webSiteLogs = sparkSession.read.json("/labs/laba03/weblogs.json")
      .select('uid, explode('visits).alias("visits"))
      .withColumn("url", col("visits").getField("url"))
      .drop('visits)
      .withColumn("domain", regexp_replace(callUDF("parse_url", 'url, lit("HOST")),"www.",""))
      .drop('url)
      .where(col("uid").isNotNull)

    val WebCategoryRes = webSiteLogs
      .join(webCatSource,Seq("domain"), "left")
      .groupBy('uid)
      .pivot('web_cat)
      .agg(count('uid))
      .drop('null)
      .drop('web_cat)

    val result = clients
      .join(shopCategoryRes,Seq("uid"), "left")
      .join(WebCategoryRes,Seq("uid"), "left")

    result.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.5:5432/tatiana_dvoryaninova")
      .option("dbtable", "clients")
      .option("user", "tatiana_dvoryaninova")
      .option("password", "IDke0iOn")
      .option("driver", "org.postgresql.Driver")
      .mode("overwrite")
      .save

    sparkSession.stop
  }
}