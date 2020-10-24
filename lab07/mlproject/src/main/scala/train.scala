import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

object train  extends App {

  val sparkSession = SparkSession
    .builder()
    .appName("lab07_33")
    .getOrCreate()

  import sparkSession.implicits._
  val webSiteLogs = sparkSession.read.json("/labs/laba07/laba07.json")
    .select('uid, 'gender_age, explode('visits).alias("visits"))
    .withColumn("url", col("visits").getField("url"))
    .drop("visits")
    .groupBy("uid", "gender_age").agg(collect_list('url).alias("domains"))
    .select('uid, 'domains, 'gender_age)

  val cv = new CountVectorizer()
    .setInputCol("domains")
    .setOutputCol("features")

  val indexer = new StringIndexer()
    .setInputCol("gender_age")
    .setOutputCol("label")

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.001)

  val indexConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(indexer.fit(webSiteLogs).labels)

  val pipeline = new Pipeline()
    .setStages(Array(cv, indexer, lr, indexConverter))

  val model = pipeline.fit(webSiteLogs)
  model.write.overwrite().save("/user/tatiana.dvoryaninova/model_test1")

}