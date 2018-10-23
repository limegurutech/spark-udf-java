package com.limeguru.tutorial;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

public class MultiColumnUDF {

	@SuppressWarnings({ "serial" })
	public static void main(String[] args) {
		try {
			// setup spark configuration and create spark context/spark session
			SparkConf sparkConf = new SparkConf().setAppName("single-column-udf").setMaster("local[*]");
			SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

			// Load dataset
			Dataset<Row> products = sparkSession.read().format("csv").option("header", "true")
					.load("src/main/resources/products.csv");

			// display the products before applying UDF
			products.show(false);

			// Create udf to concatenate 2 columns (UDF2 is used)- category and subcategory
			UDF2<String, String, String> categoryHierarchy = new UDF2<String, String, String>() {
				// @Override
				public String call(String category, String subcategory) throws Exception {
					return category + "->" + subcategory;
				}
			};

			// register udf in your spark session
			sparkSession.udf().register("category_hierarchy", categoryHierarchy, DataTypes.StringType);

			// use the udf by passing 2 columns and getting the output in category_hierarchy
			products = products.withColumn("category_hierarchy",
					callUDF("category_hierarchy", col("category"), col("subcategory")));

			// display the products after applying UDF
			products.show(false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
