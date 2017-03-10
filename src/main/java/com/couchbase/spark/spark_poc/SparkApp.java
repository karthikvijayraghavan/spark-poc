package com.couchbase.spark.spark_poc;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;



public class SparkApp {

	public static void main(String[] args) {
		
		 String pNodes = args[0];
         String pBucket = args[1];
         String pPassword = args[2];
         String csvFile = args[3];
		SparkApp obj = new SparkApp();

		System.setProperty("com.couchbase.kvTimeout", "9000");
		System.setProperty("com.couchbase.connectTimeout", "9000");
		System.setProperty("com.couchbase.socketConnect", "8000");

//		SparkConf conf = new SparkConf().setAppName("Spark Application").setMaster("local[*]")
//				.set("spark.couchbase.nodes",
//						"ec2-52-34-70-26.us-west-2.compute.amazonaws.com;ec2-52-26-231-227.us-west-2.compute.amazonaws.com;ec2-52-10-172-192.us-west-2.compute.amazonaws.com;ec2-35-167-134-86.us-west-2.compute.amazonaws.com")
//				.set("spark.couchbase.bucket.default", "");

		
		SparkSession spark = SparkSession
				.builder()
				.appName("Spark Application")
				.master("local[*]") // use the JVM as the master, great for testing
				.config("spark.couchbase.nodes", pNodes) // connect to couchbase on localhost
				.config("spark.couchbase.bucket."+pBucket, pPassword) // open the bucket with empty password
				.getOrCreate();
		
		// The Java wrapper around the SparkContext
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		
		//JavaSparkContext sc = new JavaSparkContext(conf);
		Spark sparkInstance = new Spark(sc);
		sparkInstance.csvToCouchbase(obj.getClass().getClassLoader().getResource(csvFile).getFile());

	}

}
