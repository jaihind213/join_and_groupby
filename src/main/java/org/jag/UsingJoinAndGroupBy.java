package org.jag;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UsingJoinAndGroupBy {

  private static Logger logger = LoggerFactory.getLogger(UsingJoinAndGroupBy.class);

  public static void main(String[] args) {
    String peopleInterestPath = args[0];
    String peopleLocationPath = args[1];
    String outputPath = args[2];
    boolean useApproxDistinctCount = true;

    if (args.length > 3) {
      useApproxDistinctCount = Boolean.parseBoolean(args[3]);
    }
    System.out.println("Using approx distinct count: " + useApproxDistinctCount);

    String master = System.getProperty("spark.master", "local[*]");
    SparkSession spark = null;
    if (master.contains("local[")) {
      System.out.println("using spark Master: local[*]");
      spark = SparkSession.builder().master("local[*]").getOrCreate();
    } else {
      spark = SparkSession.builder().getOrCreate();
    }

    try {
      final String technique = useApproxDistinctCount ? "approx_distinct_count" : "distinct_count";
      final String resultPath = outputPath + "/" + "UsingJoinAndGroupBy_" + technique;

      final long start = System.currentTimeMillis();
      Dataset<Row> interests = spark.read().parquet(peopleInterestPath); // .repartition(8);
      Dataset<Row> locations = spark.read().parquet(peopleLocationPath); // .repartition(8);
      Dataset<Row> result = joinAndGroupBy(spark, interests, locations, useApproxDistinctCount);
      result.write().mode("overwrite").option("header", "true").csv(resultPath);
      final long end = System.currentTimeMillis();
      System.out.println("Time taken Ms: " + (end - start));

      Utils.recordTimeToFile(
          (end - start), "UsingJoinAndGroupBy_" + technique, outputPath + "/results.csv");
    } finally {
      if (spark != null) {
        spark.close();
      }
    }
  }

  static Dataset<Row> joinAndGroupBy(
      SparkSession spark,
      Dataset<Row> interests,
      Dataset<Row> locations,
      boolean approxCountDistinct) {
    interests.createOrReplaceTempView("interests");
    locations.createOrReplaceTempView("locations");

    final String functionToUse =
        approxCountDistinct ? "approx_count_distinct(pid)" : "count(distinct(pid))";
    final String colName = approxCountDistinct ? "approx_pid_count" : "distinct_count";

    String sql =
        String.format(
            "WITH exploded_table AS (\n"
                + "    SELECT t1.pid, \n"
                + "           EXPLODE(t1.locations) AS location,\n"
                + "           t2.interest\n"
                + "    FROM interests t2\n"
                + "    JOIN locations t1\n"
                + "    ON t1.pid = t2.pid\n"
                + ")\n"
                + "\n"
                + "SELECT location, \n"
                + "       interest, \n"
                + "       %s AS %s\n"
                + "FROM exploded_table\n"
                + "GROUP BY location, interest",
            functionToUse, colName);

    logger.info("Executing SQL: {}", sql);

    return spark.sql(sql);
  }
}
