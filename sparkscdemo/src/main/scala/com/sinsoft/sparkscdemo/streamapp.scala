package com.sinsoft.sparkscdemo

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

object streamapp extends App {

  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(1));

  val line = ssc.socketTextStream("192.168.11.123", 9999, StorageLevel.MEMORY_AND_DISK_SER)
  val words = line.flatMap(_.split(" "))

  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)

  wordCounts.print()
  println(wordCounts.toString())

  ssc.start();
  ssc.awaitTermination();
}