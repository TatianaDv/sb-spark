import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions

object features  extends App {
  val sparkSession = SparkSession
    .builder()
    .appName("lab06_33")
    .getOrCreate()
  import sparkSession.implicits._

  val webSiteLogs = sparkSession.read.json("/labs/laba03/weblogs.json")
    .select('uid, explode('visits).alias("visits"))
    .withColumn("url", col("visits").getField("url"))
    .withColumn("timestamp", col("visits").getField("timestamp"))
    .withColumn("date", to_timestamp(to_utc_timestamp(($"timestamp"/1000).cast("timestamp"), "Europe/Moscow"),"YYYYMMDD HH:mm:ss"))
    .withColumn("hour", hour(col("date")))
    .withColumn("web_hour", concat(lit("web_hour_"),hour(col("date"))))
    .withColumn("web_day", lower(concat(lit("web_day_"),date_format(col("date"), "E"))))
    .withColumn("web_fract", when(col("hour") >= 9 && col("hour") < 18, "WORK")
      .when(col("hour") >= 18 && col("hour") <= 23, "EVENING").otherwise("Unknown"))
    .drop('visits)
    .drop('timestamp)
    .withColumn("domain", regexp_replace(lower(callUDF("parse_url", 'url, lit("HOST"))),"www.",""))
    .drop('url)
    .where(col("uid").isNotNull)
    .where(col("domain").isNotNull)

  val cWork = webSiteLogs
    .select('uid, 'web_fract)
    .filter('web_fract === "WORK")
    .groupBy('uid)
    .agg(count('uid))
    .select('uid, functions.col("count(uid)").alias("web_count_work_hours"))
  val cEvening = webSiteLogs
    .select('uid, 'web_fract)
    .filter('web_fract === "EVENING")
    .groupBy('uid)
    .agg(count('uid))
    .select('uid, functions.col("count(uid)").alias("web_count_evening_hours"))
  val cAllHours = webSiteLogs
    .select('uid, 'web_fract)
    .groupBy('uid)
    .agg(count('uid))
    .select('uid, functions.col("count(uid)").alias("web_count_all_hours"))
  //.alias("web_fraction_all_hours")
  val fWork = cAllHours
    .join(cWork, cWork("uid") === cAllHours("uid"), "left")
    .withColumn("web_fraction_work_hours", 'web_count_work_hours / 'web_count_all_hours)
    .drop(cWork("uid"))
    .select('uid,'web_fraction_work_hours)
  val fEvening = cAllHours
    .join(cEvening, cEvening("uid") === cAllHours("uid"), "left")
    .withColumn("web_fraction_evening_hours", 'web_count_evening_hours / 'web_count_all_hours)
    .drop(cEvening("uid"))
    .select('uid,'web_fraction_evening_hours)
  val webSiteLogsFract = fWork
    .join(fEvening, fEvening("uid") === fWork("uid"), "left")
    .drop(fEvening("uid"))

  val cDay = webSiteLogs
    .select('uid, 'web_day)
    .groupBy('uid)
    .pivot('web_day)
    .agg(count('uid))
  val fDay = cDay
    .join(webSiteLogsFract, webSiteLogsFract("uid") === cDay("uid"), "left")
    .drop(webSiteLogsFract("uid"))
  val cHour = webSiteLogs
    .select('uid, 'web_hour)
    .groupBy('uid)
    .pivot('web_hour)
    .agg(count('uid))
  val fDayHour = fDay
    .join(cHour, cHour("uid") === fDay("uid"), "left")
    .drop(cHour("uid"))

  val userItems = sparkSession
    .read
    .format("parquet")
    .load("/user/tatiana.dvoryaninova/users-items/*")
  val userItemsWithFields = userItems
    .join(fDayHour, fDayHour("uid") === userItems("uid"), "left")
    .drop(fDayHour("uid"))

  val webCntTop1000 = webSiteLogs
    .groupBy("domain").count()
    .select('domain, functions.col("count").alias("countAll"))
    .orderBy(-'countAll).limit(1000)
  //webCnt.show(5)

  val userList = webSiteLogs // список уникальных uid
    .select("uid")
    .distinct //35 960

  val webCntUser = webSiteLogs//статистика по всем сайтам для пользователей
    .groupBy("uid","domain").count()
    .select('uid,'domain, functions.col("count").alias("countUser"))

  val userListWithTop1000 = webCntTop1000.crossJoin(userList) // для каждого uid 1000 сайтов cnt-35960000
  //userListWithTop1000.show(5)

  val webCntUserForTop1000 = userListWithTop1000
    .join(webCntUser, Seq("uid", "domain"), "left")
    .na.fill(0)
    .orderBy('uid,'domain,-'countAll)
    .groupBy("uid").agg(collect_list('countUser).alias("domain_features"))
    .orderBy('uid)

  val resList = userItemsWithFields
    .join(webCntUserForTop1000, Seq("uid"), "inner")
    .na.fill(0)

  // Результат пишем в файл
  resList
    .write
    .format("parquet")
    .mode("overwrite")
    .save("/user/tatiana.dvoryaninova/features")

}
