import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.hadoop.security.UserGroupInformation
import io.delta.tables._
// import spark.implicits._

object StreamingDemo {
  def main(args: Array[String]): Unit = {

    // UserGroupInformation.setLoginUser(
    //   UserGroupInformation.createRemoteUser("chas")
    // )

    val spark = SparkSession.builder
      .appName("Autoscaling Demo")
      .config("spark.sql.adaptive.enabled", "false")
      // .config("spark.master", "local[16]") // local dev
      // .config(
      //   "spark.hadoop.fs.AbstractFileSystem.gs.impl",
      //   "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
      // )
      // .config("spark.hadoop.fs.gs.project.id", "cf-data-analytics")
      // .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      // .config(
      //   "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
      //   "/Users/chasf/Desktop/cf-data-analytics-f8ccb6c85b39.json"
      // )
      .getOrCreate()

    import spark.implicits._

    // val test =
    //   spark.read.format("delta").load("gs://cf-data-temp/spark-delta/")

    // test.head()

    // print("done")
    val simpleSchema = StructType(
      Array(
        StructField("name", StringType, true),
        StructField("id", StringType, true),
        StructField("time", StringType, true)
      )
    )

    // // val df =
    // //   spark.readStream
    // //     .schema(simpleSchema)
    // //     .json("gs://cf-data-temp/spark-input/*")
    // //     .withColumn("event_time", to_timestamp($"time"))

    val df_delta =
      spark.readStream
        .schema(simpleSchema)
        .json("gs://cf-data-temp/spark-input/*")
        .withColumn("event_time", to_timestamp($"time"))

    // df.writeStream
    //   .format("bigquery")
    //   .option("temporaryGcsBucket", "cf-spark-temp")
    //   // .option("writeMethod", "direct")
    //   .option("checkpointLocation", "gs://cf-data-temp/spark-checkpoint/")
    //   .outputMode("append")
    //   .start(
    //     "cf-data-analytics.spark_example.streaming"
    //   )
    // print(current_timestamp())
    // // df_delta.show()

    val df_out = df_delta
      // .withColumn("timestamp", current_timestamp())
      .withWatermark("event_time", "5 minutes")
      .groupBy(window($"event_time", "1 minutes"))
      .count() // streaming transformation

    // // df_out.show()

    val query = df_out.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "gs://cf-data-temp/spark-checkpoint-delta/")
      .start(
        "gs://cf-data-temp/spark-delta/"
      )

    print(query.lastProgress)

    // print(query.explain)

    // // df.writeStream
    // //   .format("delta")
    // //   .outputMode("append")
    // //   .option("checkpointLocation", "gs://cf-data-temp/spark-checkpoint-delta/")
    // //   .start(
    // //     "gs://cf-data-temp/spark-delta/"
    // //   )

    spark.streams
      .awaitAnyTermination() // block until any one of the streams terminates

    // val df_delta =
    //   spark.read
    //     .schema(simpleSchema)
    //     .json("gs://cf-data-temp/spark-input/*")
    //     .withColumn("event_time", to_timestamp($"time"))

    // // df_delta.head()

    // print("done")

    // val df_out = df_delta
    //   // .withColumn("timestamp", current_timestamp())
    //   // .withWatermark("timestamp", "5 minutes")
    //   .groupBy(window($"timestamp", "1 minutes"))
    //   .count() // streaming transformation

    // df_out.write
    //   .format("delta")
    //   // .outputMode("append")
    //   // .option("checkpointLocation", "gs://cf-data-temp/spark-checkpoint-delta/")
    //   .save(
    //     "gs://cf-data-temp/spark-delta/"
    //   )
    // df_out.explain()

    // print("done")

  }
}
