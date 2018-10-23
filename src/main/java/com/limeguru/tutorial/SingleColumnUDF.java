package com.limeguru.tutorial;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;

public class SingleColumnUDF {

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

			// Create udf to prefix 00 in single column (UDF1 is used)
			UDF1<String, String> formatId = new UDF1<String, String>() {

				// @Override
				public String call(String id) throws Exception {
					return "00" + id;
				}
			};

			// register udf in your spark session
			sparkSession.udf().register("format_id", formatId, DataTypes.StringType);

			// use udf to pass id column as input and get the output in new column -
			// formatted_id
			products = products.withColumn("formatted_id", callUDF("format_id", col("id")));

			// display the products after applying UDF
			products.show(false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
