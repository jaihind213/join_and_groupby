package org.jag.generate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.jag.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class generates the person location data by reading the person data and generating random
 * locations for each person. Schema of the generated data: pid<String>, locations<Array<String>>
 */
public class PersonLocationDataGenerator {

  private static Logger logger = LoggerFactory.getLogger(PersonLocationDataGenerator.class);

  public static void main(String[] args) {
    String inputPeoplePath = args[0];
    String outPutPath = args[1];

    String master = System.getProperty("spark.master", "local[*]");
    SparkSession spark = null;
    if (master.contains("local[")) {
      System.out.println("using spark Master: local[*]");
      spark = SparkSession.builder().master("local[*]").getOrCreate();
    } else {
      spark = SparkSession.builder().getOrCreate();
    }

    long startTime = System.currentTimeMillis();
    try {
      Dataset<Row> peopleDF = spark.read().parquet(inputPeoplePath);

      // suggest to repartition the data according to number of cores u have.
      final int numPartitions = Integer.parseInt(System.getProperty("re_partitions", "8"));
      logger.info(
          "Will repartition the data into {} parts and write it to the final location.",
          numPartitions);

      Dataset<Row> locations =
          peopleDF.mapPartitions(
              (MapPartitionsFunction<Row, Row>)
                  iterator -> {
                    List<Row> rows = new ArrayList<>();
                    while (iterator.hasNext()) {
                      Row row = iterator.next();
                      String pid = row.getString(0);
                      rows.add(RowFactory.create(pid, getRandomLocations().toArray()));
                    }
                    return rows.iterator();
                  },
              RowEncoder.apply(Schema.personLocationsSchema));

      if (numPartitions > 0) {
        logger.info(
            "Will repartition the data into {} parts and write it to the final location.",
            numPartitions);
        locations.repartition(numPartitions).write().mode(SaveMode.Overwrite).parquet(outPutPath);
      } else {
        locations.write().mode(SaveMode.Overwrite).parquet(outPutPath);
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

  static List<String> getRandomLocations() {
    List<String> citiesGrup1 = new ArrayList<>();
    // length 9
    citiesGrup1.add("Anchorage");
    citiesGrup1.add("Singapore");
    citiesGrup1.add("Pyongyang");
    citiesGrup1.add("Amsterdam");
    citiesGrup1.add("Bucharest");
    citiesGrup1.add("Marseille");
    citiesGrup1.add("Barcelona");
    citiesGrup1.add("Stockholm");
    citiesGrup1.add("Edinburgh");
    citiesGrup1.add("Lisbon");
    citiesGrup1.add("Budapest");
    citiesGrup1.add("Copenhagen");
    citiesGrup1.add("Dublin");
    citiesGrup1.add("Helsinki");
    citiesGrup1.add("Oslo");
    citiesGrup1.add("Prague");
    citiesGrup1.add("Reykjavik");

    List<String> citiesGrup2 = new ArrayList<>();
    // length 6
    citiesGrup2.add("London");
    citiesGrup2.add("Bombay");
    citiesGrup2.add("Moscow");
    citiesGrup2.add("Sydney");
    citiesGrup2.add("Tehran");
    citiesGrup2.add("Nagpur");
    citiesGrup2.add("Phuket");
    citiesGrup2.add("Austin");
    citiesGrup2.add("Warsaw");
    citiesGrup2.add("Kiev");
    citiesGrup2.add("Rome");
    citiesGrup2.add("Paris");
    citiesGrup2.add("Berlin");
    citiesGrup2.add("Vienna");
    citiesGrup2.add("Madrid");
    citiesGrup2.add("Athens");

    // Shuffle the list to randomize the order
    Collections.shuffle(citiesGrup2);
    Collections.shuffle(citiesGrup1);

    List<String> cities = new ArrayList<>();
    cities.addAll(citiesGrup1.subList(0, 5));
    cities.addAll(citiesGrup2.subList(0, 5));
    return cities;
  }
}
