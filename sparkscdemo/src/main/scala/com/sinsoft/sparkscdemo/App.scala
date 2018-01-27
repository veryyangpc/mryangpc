package com.sinsoft.sparkscdemo

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 * 
 * spark sql demo
 */
object App {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    println("Hello World!")
    println("concat arguments = " + foo(args))
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
