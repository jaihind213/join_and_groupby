package org.jag;

import static org.apache.spark.sql.functions.*;
import static org.jag.hll.dsketch.DatasketchesHLLMergeAggregator.HLL_MERGE_FUNC_NAME;
import static org.jag.hll.dsketch.DatasketchesHllAggregator.HLL_FUNCTION_NAME;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.jag.hll.aggKnw.AggregateKnowledgeHllAggregator;
import org.jag.hll.aggKnw.AggregateKnowledgeHllUnionAggregator;
import org.jag.hll.dsketch.DatasketchesHLLMergeAggregator;
import org.jag.hll.dsketch.DatasketchesHllAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * We achieve join & groupBy - distinct count using HLL. You have the option to use different
 * libraries for HLL implementation. You can also choose to use different bucketing techniques -
 * Physical Partitioning or Spark BucketBy. You can break the join by bucketing the data by pid into
 * 'n' buckets & you can even run the bucketing in parallel.
 */
public class UsingJoinAndGroupByHLL {

  // todo code is a little dirty ... clean it up :) was doing a quick test
  private static Logger logger = LoggerFactory.getLogger(UsingJoinAndGroupByHLL.class);

  public static void main(String[] args) {
    String peopleInterestPath = args[0];
    String peopleLocationPath = args[1];
    String outputPath = args[2];
    int numBuckets =
        System.getenv("NUM_BUCKETS") != null ? Integer.parseInt(System.getenv("NUM_BUCKETS")) : 1;
    int numParallelJobs =
        System.getenv("NUM_PARALLEL_BUCKETS") != null
            ? Integer.parseInt(System.getenv("NUM_PARALLEL_BUCKETS"))
            : 2;

    System.out.println("Number of buckets: " + numBuckets);

    String master = System.getProperty("spark.master", "local[*]");
    SparkSession spark = null;
    if (master.contains("local[")) {
      System.out.println("using spark Master: local[*]");
      spark =
          SparkSession.builder()
              .master("local[*]")
              .config("spark.local.dir", outputPath + "/tmp")
              .config("spark.sql.warehouse.dir", outputPath + "/spark-warehouse")
              .getOrCreate();
    } else {
      spark =
          SparkSession.builder()
              .config("spark.local.dir", outputPath + "/tmp")
              .config("spark.sql.warehouse.dir", outputPath + "/spark-warehouse")
              .getOrCreate();
    }

    final String sketchLib =
        StringUtils.isBlank(System.getenv("SKETCH_LIBRARY"))
            ? "data_sketches"
            : System.getenv("SKETCH_LIBRARY");
    final String bucketingTechnique =
        StringUtils.isBlank(System.getenv("BUCKET_TECHNIQUE"))
            ? "physical"
            : System.getenv("BUCKET_TECHNIQUE");
    final int logK = System.getenv("LOG_K") != null ? Integer.parseInt(System.getenv("LOG_K")) : 13;
    final int registerWidth =
        System.getenv("REGISTER_WIDTH") != null
            ? Integer.parseInt(System.getenv("REGISTER_WIDTH"))
            : 4;
    System.out.println(
        "Using sketch library: "
            + sketchLib
            + " logK: "
            + logK
            + " registerWidth: "
            + registerWidth);
    System.out.println("Using bucketing technique: " + bucketingTechnique);
    registerUdfs(spark, logK, 2131319, registerWidth, sketchLib);

    try {
      final String technique =
          (numBuckets > 1
                  ? "hll_buckets_"
                      + bucketingTechnique
                      + "_"
                      + numBuckets
                      + "_parallel_"
                      + numParallelJobs
                  : "hll")
              + "_"
              + sketchLib;
      final String resultPath = outputPath + "/" + "UsingJoinAndGroupBy_" + technique;

      final long start = System.currentTimeMillis();
      Dataset<Row> interests = spark.read().parquet(peopleInterestPath);
      Dataset<Row> locations = spark.read().parquet(peopleLocationPath);

      Dataset<Row> result = null;
      if (numBuckets > 1) {
        if (bucketingTechnique.equals("physical")) {
          result =
              doPhysicalBuckeitng(
                  spark, numBuckets, locations, interests, outputPath, numParallelJobs);
        } else {
          spark.conf().set("spark.sql.sources.bucketing.enabled", "true");
          spark.conf().set("spark.sql.autoBroadcastJoinThreshold", "-1");

          spark.sql("DROP TABLE IF EXISTS bucketed_table_locations"); // .show(1);
          spark.sql("DROP TABLE IF EXISTS bucketed_table_interests"); // .show(1);

          locations.createOrReplaceTempView("temp_locations");
          locations = spark.sql("select pid, explode(locations) as location from temp_locations");

          locations
              .write()
              .mode(SaveMode.Overwrite)
              .format("parquet")
              .bucketBy(numBuckets, "pid")
              .sortBy(
                  "pid") // sort by helped a bit in performance thought not useful theorectically
              .option("path", outputPath + "/bucketed_table_locations")
              .saveAsTable("bucketed_table_locations");
          interests
              .write()
              .mode(SaveMode.Overwrite)
              .format("parquet")
              .bucketBy(numBuckets, "pid")
              .sortBy(
                  "pid") // sort by helped a bit in performance thought not useful theorectically
              .option("path", outputPath + "/bucketed_table_interests")
              .saveAsTable("bucketed_table_interests");
          String sql =
              "select l.location, i.interest, hll_of(i.pid) as hll_bytes from bucketed_table_interests"
                  + " i join bucketed_table_locations"
                  + " l on i.pid = l.pid group by location, interest";
          result = spark.sql(sql);
        }
      } else {
        locations.createOrReplaceTempView("temp_locations");
        locations = spark.sql("select pid, explode(locations) as location from temp_locations");
        result = joinAndGroupBy(spark, interests, locations, 0);
      }
      result.sparkSession().sparkContext().setJobDescription("saving_merged_hll_final_result");
      result.write().mode(SaveMode.Overwrite).parquet(resultPath);
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
      SparkSession spark, Dataset<Row> interests, Dataset<Row> locations, int bucket) {
    interests.createOrReplaceTempView("interests_" + bucket);
    locations.createOrReplaceTempView("locations_" + bucket);

    String sql =
        "select l.location, i.interest, hll_of(i.pid) as hll_bytes from interests_"
            + bucket
            + " i join locations_"
            + bucket
            + " l on i.pid = l.pid group by location, interest";

    logger.info("****Executing SQL: {}", sql);
    try {
      return spark.sql(sql);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public static void registerUdfs(
      SparkSession spark, int logK, int registerWidth, int seed, String libType) {
    libType = StringUtils.isBlank(libType) ? "data_sketches" : libType;
    switch (libType.toLowerCase()) {
      case "data_sketches":
        System.out.println(
            "Using sketch Libary: data_sketches: logK:" + logK + " ,registerWidth" + registerWidth);
        TgtHllType tgtHllType = TgtHllType.HLL_4;
        if (registerWidth == 6) {
          tgtHllType = TgtHllType.HLL_6;
        } else if (registerWidth == 8) {
          tgtHllType = TgtHllType.HLL_8;
        }
        DatasketchesHllAggregator aggregator = new DatasketchesHllAggregator(logK, tgtHllType);
        DatasketchesHLLMergeAggregator mergeAggregator =
            new DatasketchesHLLMergeAggregator(logK, tgtHllType);
        DatasketchesHllAggregator.EstimateHllUdf estimateHllUdf =
            new DatasketchesHllAggregator.EstimateHllUdf();
        spark
            .udf()
            .register(
                HLL_FUNCTION_NAME,
                org.apache.spark.sql.functions.udaf(aggregator, Encoders.STRING()));

        spark
            .udf()
            .register(
                HLL_MERGE_FUNC_NAME,
                org.apache.spark.sql.functions.udaf(mergeAggregator, Encoders.BINARY()));

        spark.udf().register("estimate_hll", estimateHllUdf, DataTypes.DoubleType);
        break;
      default:
        System.out.println("Using sketch Libary: aggregateKnowledge");
        AggregateKnowledgeHllAggregator aggKnowledgeAggregator =
            new AggregateKnowledgeHllAggregator();
        aggKnowledgeAggregator.setSeed(seed);
        aggKnowledgeAggregator.setLog2m(logK);
        aggKnowledgeAggregator.setRegWidth(registerWidth);

        AggregateKnowledgeHllUnionAggregator hllUnionAggregator =
            new AggregateKnowledgeHllUnionAggregator();
        hllUnionAggregator.setLog2m(logK);
        hllUnionAggregator.setRegWidth(registerWidth);

        AggregateKnowledgeHllAggregator.EstimateHllUdf estimator =
            new AggregateKnowledgeHllAggregator.EstimateHllUdf();

        spark
            .udf()
            .register(
                HLL_FUNCTION_NAME,
                org.apache.spark.sql.functions.udaf(aggKnowledgeAggregator, Encoders.STRING()));

        spark
            .udf()
            .register(
                HLL_MERGE_FUNC_NAME,
                org.apache.spark.sql.functions.udaf(hllUnionAggregator, Encoders.BINARY()));

        spark.udf().register("estimate_hll", estimator, DataTypes.LongType);
    }
  }

  static Dataset<Row> doPhysicalBuckeitng(
      SparkSession spark,
      int numBuckets,
      Dataset<Row> locations,
      Dataset<Row> interests,
      String outputPath,
      int numParallelJobs) {
    System.out.println("We will attempt to bucket the data into 'n' parts/bucket, n=" + numBuckets);
    locations = locations.withColumn("bucket_by_pid", abs(hash(col("pid"))).mod(numBuckets));

    locations.createOrReplaceTempView("temp_locations");
    locations =
        spark.sql("select pid, explode(locations) as location, bucket_by_pid from temp_locations");

    locations.sparkSession().sparkContext().setJobDescription("bucketing_locations_by_pid");
    locations
        .write()
        .partitionBy("bucket_by_pid")
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/scrap_locations");

    interests = interests.withColumn("bucket_by_pid", abs(hash(col("pid"))).mod(numBuckets));
    interests.sparkSession().sparkContext().setJobDescription("bucketing_interests_by_pid");
    interests
        .write()
        .partitionBy("bucket_by_pid")
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/scrap_interests");

    ThreadPoolExecutor threadPool =
        (ThreadPoolExecutor) Executors.newFixedThreadPool(numParallelJobs);
    final SparkSession sparkSession = spark;
    for (int i = 0; i < numBuckets; i++) {
      final int bkt = i;
      try {
        threadPool.submit(
            new Runnable() {
              @Override
              public void run() {
                Dataset<Row> intr =
                    sparkSession
                        .read()
                        .parquet(outputPath + "/scrap_interests/bucket_by_pid=" + bkt);
                Dataset<Row> locs =
                    sparkSession
                        .read()
                        .parquet(outputPath + "/scrap_locations/bucket_by_pid=" + bkt);
                Dataset<Row> bucketResult = joinAndGroupBy(sparkSession, intr, locs, bkt);

                bucketResult
                    .sparkSession()
                    .sparkContext()
                    .setJobDescription("saving_bucket_result_" + bkt);
                bucketResult
                    .write()
                    .mode(SaveMode.Overwrite)
                    .parquet(outputPath + "/staging/bucket_by_pid=" + bkt);
              }
            });
      } catch (Exception e) {
        if (!Utils.IsPathNotFound(e)) {
          throw new RuntimeException();
        }
        e.printStackTrace();
        ;
        // bucket not found. continue
      }
    }

    threadPool.shutdown();
    try {
      threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    Dataset<Row> bucketResult = spark.read().parquet(outputPath + "/staging/*");
    bucketResult.createOrReplaceTempView("bucketResults");
    return spark.sql(
        "SELECT location, interest, hll_merge(hll_bytes) as hll_bytes FROM bucketResults"
            + " GROUP BY location, interest");
  }
}

/* 100millin

UsingJoinAndGroupBy_hll_data_sketches_100mill,4562719
UsingSetIntersection_nominal_entries_8192_100mill,95862
UsingSetIntersection_nominal_entries_16384,102010

Dataset 1 Path: /Users/vishnuch/work/gitcode/join_and_groupby/data/results/UsingJoinAndGroupBy_hll_data_sketches
Dataset 2 Path: /Users/vishnuch/work/gitcode/join_and_groupby/data/results/UsingJoinAndGroupBy_distinct_count
Minimum Percentage Difference: 0.012283397188150767
Maximum Percentage Difference: 10.127456804922208
Average Percentage Difference: 2.610049167395958

Dataset 1 Path: /Users/vishnuch/work/gitcode/join_and_groupby/data/results/UsingSetIntersection_16384
Dataset 2 Path: /Users/vishnuch/work/gitcode/join_and_groupby/data/results/UsingJoinAndGroupBy_distinct_count
Minimum Percentage Difference: 3.09383343335418E-4
Maximum Percentage Difference: 4.296094126041277
Average Percentage Difference: 1.2026570879953384
 */
