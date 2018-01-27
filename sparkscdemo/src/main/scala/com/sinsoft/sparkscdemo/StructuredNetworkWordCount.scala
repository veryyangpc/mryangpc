package com.sinsoft.sparkscdemo

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object StructuredNetworkWordCount extends App {

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val lines = spark.readStream
    .format("socket")
    .option("host", "192.168.11.123")
    .option("port", 9999)
    .load()

  val words = lines.as[String].flatMap(_.split(" "))

  val wordCounts = words.groupBy("value").count()

  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()
}