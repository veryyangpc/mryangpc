package com.sinsoft.sparkscdemo.sparksql

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 * 
 * spark sql demo
 */
object App {

  def main(args: Array[String]) {
    println("Hello World!")
    val spark = SparkSession
      .builder().master("local[1]")
      .appName("Spark SQL basic example")
      .getOrCreate()
      
    val df = spark.read.json("C:\\zonemycd\\workspace\\sparkscdemo\\input\\readjson.json")
    df.show()
    
    df.printSchema()
    
    df.select("name", "password").show()
    
    df.filter("age>21").show()

    df.groupBy("age").count().show()
    
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
  }

}
