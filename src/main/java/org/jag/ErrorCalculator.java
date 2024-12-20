package org.jag;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class ErrorCalculator {
  public static void main(String[] args) throws Exception {

    String dataset1Path = args[0];
    String dataset2Path = args[1];

    System.out.println("Dataset 1 Path: " + dataset1Path);
    System.out.println("Dataset 2 Path: " + dataset2Path);
    Thread.sleep(10000);

    SparkSession spark =
        SparkSession.builder()
            .appName("Percentage Difference Calculation")
            .master("local[*]") // Use appropriate cluster configuration
            .getOrCreate();

    // Load the datasets
    Dataset<Row> df1 =
        spark.read().option("header", "true").option("inferSchema", "true").csv(dataset1Path);

    String[] columns = df1.columns();
    String countColumnName = "";
    for (String column : columns) {
      if (column.toLowerCase().contains("count")) {
        countColumnName = column;
        break;
      }
    }
    df1 = df1.withColumnRenamed(countColumnName, ("count_1"));
    df1 = df1.withColumnRenamed("interest", ("interest1"));
    df1 = df1.withColumnRenamed("location", ("location1"));

    Dataset<Row> df2 =
        spark.read().option("header", "true").option("inferSchema", "true").csv(dataset2Path);

    columns = df2.columns();
    countColumnName = "";
    for (String column : columns) {
      if (column.toLowerCase().contains("count")) {
        countColumnName = column;
        break;
      }
    }

    df2 = df2.withColumnRenamed(countColumnName, ("count_2"));
    df2 = df2.withColumnRenamed("interest", ("interest2"));
    df2 = df2.withColumnRenamed("location", ("location2"));

    // Perform join on location and interest
    Dataset<Row> joinedDf =
        df1.join(
            df2,
            df1.col("location1")
                .equalTo(df2.col("location2"))
                .and(df1.col("interest1").equalTo(df2.col("interest2"))),
            "inner");

    joinedDf.show(100000, false);
    // Calculate percentage difference
    Dataset<Row> resultDf =
        joinedDf.withColumn(
            "percent_difference", expr("ABS(count_1 - count_2) / ((count_1 + count_2) / 2) * 100"));

    // Save the result to a new CSV file
    resultDf
        .coalesce(1)
        .write()
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv("comparison_sets_result");

    // Show the result (for debugging or visualization)
    resultDf
        .select("location2", "interest2", "count_1", "count_2", "percent_difference")
        .show(10000, false);

    Row statsRow =
        resultDf
            .agg(
                min("percent_difference").alias("min_percent_difference"),
                max("percent_difference").alias("max_percent_difference"),
                avg("percent_difference").alias("avg_percent_difference"))
            .first();

    // Extract and print min and max percentage difference
    double minDifference = statsRow.getAs("min_percent_difference");
    double maxDifference = statsRow.getAs("max_percent_difference");
    double avgDifference = statsRow.getAs("avg_percent_difference");

    //
    System.out.println("Dataset 1 Path: " + dataset1Path);
    System.out.println("Dataset 2 Path: " + dataset2Path);
    System.out.println("Minimum Percentage Difference: " + minDifference);
    System.out.println("Maximum Percentage Difference: " + maxDifference);
    System.out.println("Average Percentage Difference: " + avgDifference);

    // Stop Spark session
    spark.stop();
  }
}
