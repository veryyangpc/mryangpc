package com.sinosoft.sparkjavademo.sparksql;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;



public class WordCountApp {
	
	public static void main(String[] args) {
		 String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
		    SparkSession spark = SparkSession.builder().appName("Simple Application")
		    		.master("local[2]").getOrCreate();
		    Dataset<String> logData = spark.read().textFile(logFile).cache();

		    long numAs = logData.filter(s -> s.contains("a")).count();
		    long numBs = logData.filter(s -> s.contains("b")).count();

		    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

		    spark.stop();
		
	}
}
