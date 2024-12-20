package org.jag.generate;

import static org.jag.Schema.peopleSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.*;
import org.jag.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Generates a dataset of people with unique ids. Schema is pid: String */
public class PersonGenerator {

  private static Logger logger = LoggerFactory.getLogger(PersonGenerator.class);

  public static void main(String[] args) {
    // path to write the data
    String outPutPath = args[0];
    // number of Unique people to generate
    final int numberOfPeople = Integer.parseInt(args[1]);
    // Write the data in batches,else mite go OOM for bigger data
    final int numBatches = Integer.parseInt(args[2]);

    int batchSize = (int) Math.ceil((double) numberOfPeople / numBatches);

    String master = System.getProperty("spark.master", "local[*]");
    SparkSession spark = null;
    if (master.contains("local[")) {
      System.out.println("using spark Master: local[*]");
      spark = SparkSession.builder().master("local[*]").getOrCreate();
    } else {
      spark = SparkSession.builder().getOrCreate();
    }

    try {
      int id = 0;
      for (int batch = 0; batch < numBatches; batch++) {
        System.out.println("Batch " + (batch + 1) + ":");
        List<Row> rows = new ArrayList<>();
        // Calculate the range of elements for the current batch
        int start = batch * batchSize;
        int end = Math.min(start + batchSize, numberOfPeople);

        for (int i = start; i < end; i++) {
          rows.add(RowFactory.create((id++) + "_" + Utils.generateRandomString(35)));
        }
        Dataset<Row> peopleDF = spark.createDataFrame(rows, peopleSchema);
        peopleDF.write().mode(SaveMode.Append).save(outPutPath + "_");
      }

      // suggest to repartition the data according to number of cores u have.
      final int numPartitions = Integer.parseInt(System.getProperty("re_partitions", "8"));

      Dataset<Row> stagedPpl = spark.read().parquet(outPutPath + "_");
      if (numPartitions > 0) {
        logger.info(
            "Will repartition the data into {}, parts and write it to the final location.",
            numPartitions);
        stagedPpl.repartition(numPartitions).write().mode(SaveMode.Overwrite).parquet(outPutPath);
      } else {
        stagedPpl.write().mode(SaveMode.Overwrite).parquet(outPutPath);
      }

      try {
        // delete staging dir
        FileUtils.delete(new java.io.File(outPutPath + "_"));
      } catch (IOException ignore) {
      }

      logger.info("Number of records written: " + spark.read().parquet(outPutPath).count());
      logger.info("Path to the generated data: " + outPutPath);
    } finally {
      if (spark != null) {
        spark.close();
      }
    }
  }
}
