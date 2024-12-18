package org.jag.generate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.jag.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class generates the person interest data by reading the person data and generating random
 * interests for each person. Schema of the generated data: pid<String>, interest<String>
 */
public class PersonInterestDataGenerator {

  private static Logger logger = LoggerFactory.getLogger(PersonInterestDataGenerator.class);

  public static void main(String[] args) {
    String pathToPeopleDataset = args[0];
    String outPutPath = args[1];

    String master = System.getProperty("spark.master", "local[*]");
    SparkSession spark = null;
    if (master.contains("local[")) {
      System.out.println("using spark Master: local[*]");
      spark = SparkSession.builder().master("local[*]").getOrCreate();
    } else {
      spark = SparkSession.builder().getOrCreate();
    }

    final long startTime = System.currentTimeMillis();
    try {
      Dataset<Row> peopleDF = spark.read().parquet(pathToPeopleDataset);

      Dataset<Row> interests =
          peopleDF.flatMap(
              new FlatMapFunction<Row, Row>() {
                @Override
                public Iterator<Row> call(Row row) throws Exception {
                  List<Row> rows = new ArrayList<>();
                  String pid = row.getString(0);
                  for (String interest : getRandomInterests()) {
                    rows.add(RowFactory.create(pid, interest));
                  }
                  return rows.iterator();
                }
              },
              RowEncoder.apply(Schema.personInterestSchema));

      // suggest to repartition the data according to number of cores u have.
      final int numPartitions = Integer.parseInt(System.getProperty("re_partitions", "8"));

      if (numPartitions > 0) {
        logger.info(
            "Will repartition the data into {} parts and write it to the final location.",
            numPartitions);
        interests.repartition(numPartitions).write().mode(SaveMode.Overwrite).parquet(outPutPath);
      } else {
        interests.write().mode(SaveMode.Overwrite).parquet(outPutPath);
      }
      logger.info("Number of records written: " + spark.read().parquet(outPutPath).count());
      logger.info("Path to the generated data: " + outPutPath);
    } finally {
      if (spark != null) {
        spark.close();
      }
    }
    System.out.println("Time taken: " + (System.currentTimeMillis() - startTime) + "ms");
  }

  static List<String> getRandomInterests() {
    List<String> grup1 = new ArrayList<>();
    // length 5
    grup1.add("Pizza");
    grup1.add("Paint");
    grup1.add("Sushi");
    grup1.add("Pasta");
    grup1.add("Tacos");
    grup1.add("Ramen");
    grup1.add("Chips");
    grup1.add("Salad");
    grup1.add("Tango");

    List<String> grup2 = new ArrayList<>();
    // length 4
    grup2.add("Yoga");
    grup2.add("Chai");
    grup2.add("Cook");
    grup2.add("Cars");
    grup2.add("Lego");
    grup2.add("Bike");
    grup2.add("News");
    grup2.add("Tech");
    grup2.add("Cake");

    // Shuffle the list to randomize the order
    Collections.shuffle(grup2);
    Collections.shuffle(grup1);

    List<String> cities = new ArrayList<>();
    cities.addAll(grup1.subList(0, 5));
    cities.addAll(grup2.subList(0, 5));
    return cities;
  }
}
