package com.couchbase.spark.spark_poc;

import static com.couchbase.spark.japi.CouchbaseSparkContext.couchbaseContext;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.couchbase.spark.japi.CouchbaseSparkContext;
import com.couchbase.spark.sql.DataFrameWriterFunctions;

import scala.collection.immutable.Map;

public class Spark {

    private SQLContext sqlContext;
    private JavaSparkContext javaSparkContext;
    private CouchbaseSparkContext couchbaseSparkContext;

    public Spark(JavaSparkContext sc) {
        this.javaSparkContext = sc;
        this.sqlContext = new SQLContext(sc);
        this.couchbaseSparkContext = couchbaseContext(sc);
    }

    public void csvToCouchbase(String csvFilePath) {
        Dataset<Row> df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(csvFilePath);
   
        // Infer the schema uses the integer type for the id but we need a string
        df = df.withColumn("Id", df.col("Id").cast("string"));
        DataFrameWriterFunctions dataFrameWriterFunctions = new DataFrameWriterFunctions(df.write());
        // this option ensure the Id field will be used as key
        Map<String, String> options = new Map.Map1<String, String>("idField", "Id");
        dataFrameWriterFunctions.couchbase(options);
    }
//
//    public void getPopularNames(String gender, int threshold) {
//        String queryStr = "SELECT Name, Gender, SUM(Count) AS Total FROM `default` WHERE Gender = $1 GROUP BY Name, Gender HAVING SUM(Count) >= $2";
//        JsonArray parameters = JsonArray.create()
//            .add(gender)
//            .add(threshold);
//        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, parameters);
//        this.couchbaseSparkContext
//            .couchbaseQuery(query)
//            .foreach(queryResult -> System.out.println(queryResult));
//    }

}
