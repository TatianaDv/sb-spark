import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{udf, col}
import scala.util.Try
import org.apache.spark.sql.functions

object features  extends App {
  val sparkSession = SparkSession
    .builder()
    .appName("lab06_33")
    .getOrCreate()
  import sparkSession.implicits._

  // читаем логи, получаем основные колонки для расчетов
  val webSiteLogs = sparkSession.read.json("/labs/laba03/weblogs.json")
    .select('uid, explode('visits).alias("visits"))
    .withColumn("url", col("visits").getField("url"))
    .withColumn("timestamp", col("visits").getField("timestamp"))
    .withColumn("date", from_utc_timestamp(($"timestamp"/1000).cast("timestamp"),"Etc/GMT+3"))
    .withColumn("hour", hour(col("date")))
    .withColumn("web_hour", concat(lit("web_hour_"),hour(col("date"))))
    .withColumn("web_day", lower(concat(lit("web_day_"),date_format(col("date"), "E"))))
    .withColumn("web_fract", when(col("hour") >= 9 && col("hour") < 18, "WORK")
      .when(col("hour") >= 18 && col("hour") <= 23, "EVENING").otherwise("Unknown"))
    .drop('visits)
    .drop('timestamp)
    .withColumn("domain", regexp_replace(lower(callUDF("parse_url", 'url, lit("HOST"))),"www.",""))
    .drop('url)
    .where(col("domain").isNotNull)
    .orderBy('uid,'domain)

  //считаем топ 1000 сайтов
  val webCntTop1000 = webSiteLogs
    .groupBy("domain").count()
    .select('domain, functions.col("count").alias("countAll"))
    .orderBy(-'countAll).limit(1000)

  // список уникальных uid
  val userList = webSiteLogs.select("uid").distinct //35 960

  //статистика по всем сайтам для пользователей
  val webCntUser = webSiteLogs
    .groupBy("uid","domain").count()
    .select('uid,'domain, functions.col("count").alias("countUser"))

  // для каждого uid есть набор из 1000 сайтов cnt-35960000
  val userListWithTop1000 = webCntTop1000.crossJoin(userList)

  // соединяем в одну колонку 1000 сайтов
  val webCntUserForTop1000 = userListWithTop1000
    .join(webCntUser, Seq("uid", "domain"), "left")
    .na.fill(0)
    .orderBy('uid,'domain,-'countAll)
    .groupBy("uid")
    .agg(collect_list('countUser).alias("domain_features"))

  // считаем остальные колонки
  // количество для рабочих часов
  val workHours = webSiteLogs
    .filter('web_fract === "WORK")
    .groupBy('uid)
    .agg(count('uid))
    .select('uid, functions.col("count(uid)").alias("web_count_work_hours"))

  // количество для вечерних часов
  val eveningHours = webSiteLogs
    .filter('web_fract === "EVENING")
    .groupBy('uid)
    .agg(count('uid))
    .select('uid, functions.col("count(uid)").alias("web_count_evening_hours"))

  // общее количество часов
  val countAllHours = webSiteLogs
    .groupBy('uid)
    .agg(count('uid))
    .select('uid, functions.col("count(uid)").alias("web_count_all_hours"))

  // доля для рабочих часов
  val fractionWork = countAllHours
    .join(workHours, Seq("uid"), "left")
    .withColumn("web_fraction_work_hours", Try('web_count_work_hours / 'web_count_all_hours).getOrElse(lit(0)))

  // доля для вечерних часов
  val fractionEvening = countAllHours
    .join(eveningHours, Seq("uid"), "left")
    .withColumn("web_fraction_evening_hours", Try('web_count_evening_hours / 'web_count_all_hours).getOrElse(lit(0)))

  // количество по дням недели разворачиваем в колонки
  val countByDayOfWeek = webSiteLogs
    .groupBy('uid)
    .pivot('web_day)
    .agg(count('uid))

  // количество по часам разворачиваем в колонки
  val countByHour = webSiteLogs
    .groupBy('uid)
    .pivot('web_hour)
    .agg(count('uid))

  // читаем логи посещений и покупок с прошлой лабы 5
  val userItems = sparkSession
    .read
    .format("parquet")
    .load("/user/tatiana.dvoryaninova/users-items/*")

  // все объединяем
  var result = countByDayOfWeek
    .join(countByHour, Seq("uid"), "inner")
    .join(fractionWork, Seq("uid"), "inner")
    .join(fractionEvening, Seq("uid"), "inner")
    .join(webCntUserForTop1000, Seq("uid"), "inner")
    .join(userItems, Seq("uid"), "inner")
    .drop("web_count_all_hours")
    .drop("web_count_work_hours")
    .drop("web_count_evening_hours")
    .na.fill(0)

  // сохраняем результат
  result.write
    .format("parquet")
    .mode("overwrite")
    .save("/user/tatiana.dvoryaninova/features")

}
