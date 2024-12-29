package org.jag;

import static org.jag.SetUnionAggregator.SET_UNION_UDF_FUNCTION_NAME;

import java.util.*;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

public class UsingSetIntersection {

  public static void main(String[] args) {
    String peopleInterestPath = args[0];
    String peopleLocationPath = args[1];
    String outputPath = args[2];

    String master = System.getProperty("spark.master", "local[*]");
    SparkSession spark = null;
    if (master.contains("local[")) {
      System.out.println("using spark Master: local[*]");
      spark = SparkSession.builder().master("local[*]").getOrCreate();
    } else {
      spark = SparkSession.builder().getOrCreate();
    }

    final int nominalEntries = Integer.parseInt(System.getProperty("nominal_entries", "4096"));
    System.out.println("Nominal entries configured: " + nominalEntries);

    registerSetUdfs(spark, nominalEntries);

    try {
      final long start = System.currentTimeMillis();

      Dataset<Row> result =
          doSetIntersection(
              spark,
              spark.read().parquet(peopleInterestPath),
              spark.read().parquet(peopleLocationPath),
              nominalEntries);
      result
          .write()
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .parquet(outputPath + "/UsingSetIntersection_nominal_entries_" + nominalEntries + "/");
      final long end = System.currentTimeMillis();
      System.out.println("Time taken: " + (end - start) + "ms");

      Utils.recordTimeToFile(
          (end - start),
          "UsingSetIntersection_nominal_entries_" + nominalEntries,
          outputPath + "/results.csv");
    } finally {
      if (spark != null) {
        spark.close();
      }
    }
  }

  private static Dataset<Row> doSetIntersection(
      SparkSession spark,
      Dataset<Row> interests,
      Dataset<Row> locations,
      final int nominalEntries) {

    Dataset<Row> setsForEachInterestDataset =
        interests.mapPartitions(
            new MapPartitionsFunction<Row, Row>() {
              private static final long serialVersionUID = -7858793298789456506L;

              @Override
              public Iterator<Row> call(Iterator<Row> input) throws Exception {
                Map<String, UpdateSketch> sketchMap = new HashMap<>();
                List<Row> output = new ArrayList<>();
                while (input.hasNext()) {
                  Row r = input.next();
                  String pid = r.getString(0);
                  String interest = r.getString(1);
                  UpdateSketch sketch = sketchMap.get(interest);
                  if (sketch == null) {
                    sketch =
                        Sketches.updateSketchBuilder().setNominalEntries(nominalEntries).build();
                  }
                  sketch.update(pid);
                  sketchMap.put(interest, sketch);
                }
                for (Map.Entry<String, UpdateSketch> entry : sketchMap.entrySet()) {
                  output.add(
                      RowFactory.create(entry.getKey(), entry.getValue().compact().toByteArray()));
                }
                return output.iterator();
              }
            },
            RowEncoder.apply(
                new StructType()
                    .add("interest", DataTypes.StringType)
                    .add("sketch", DataTypes.BinaryType)));

    setsForEachInterestDataset.createOrReplaceTempView("interest_sets");

    Dataset<Row> mergedInterestSets =
        spark.sql(
            String.format(
                "select interest, %s(sketch) as set1 from interest_sets group by interest",
                SET_UNION_UDF_FUNCTION_NAME));

    Dataset<Row> setsPerLocationDataset =
        locations.mapPartitions(
            new MapPartitionsFunction<Row, Row>() {
              @Override
              public Iterator<Row> call(Iterator<Row> input) throws Exception {
                Map<String, UpdateSketch> sketchMap = new HashMap<>();
                List<Row> output = new ArrayList<>();
                while (input.hasNext()) {
                  Row r = input.next();
                  String pid = r.getString(0);
                  List<Object> locations = JavaConverters.seqAsJavaList(r.getAs("locations"));
                  for (Object location : locations) {
                    String loc = (String) location;
                    UpdateSketch sketch = sketchMap.get(loc);
                    if (sketch == null) {
                      sketch =
                          Sketches.updateSketchBuilder().setNominalEntries(nominalEntries).build();
                    }
                    sketch.update(pid);
                    sketchMap.put(loc, sketch);
                  }
                }
                for (Map.Entry<String, UpdateSketch> entry : sketchMap.entrySet()) {
                  output.add(
                      RowFactory.create(entry.getKey(), entry.getValue().compact().toByteArray()));
                }
                return output.iterator();
              }
            },
            RowEncoder.apply(
                new StructType()
                    .add("location", DataTypes.StringType)
                    .add("sketch", DataTypes.BinaryType)));
    setsPerLocationDataset.createOrReplaceTempView("location_sets");

    Dataset<Row> mergedLocationSets =
        spark.sql(
            String.format(
                "select location, %s(sketch) as set1 from location_sets group by location",
                SET_UNION_UDF_FUNCTION_NAME));

    // we don't expect a lot of rows to accumulate on the driver !
    // event 1000s are ok. it won't be millions !
    List<Row> rowList = mergedInterestSets.collectAsList();

    // Convert to HashMap
    HashMap<String, byte[]> interestSketchMap = new HashMap<>();
    for (Row row : rowList) {
      String key = row.getString(0); // key is the interest
      byte[] value = row.getAs(1); // value is the set sketch
      interestSketchMap.put(key, value);
    }

    // we don't expect a lot of rows to accumulate on the driver !
    // event 1000s are ok. it won't be millions !
    List<Row> rowList2 = mergedLocationSets.collectAsList();

    // Convert to HashMap
    HashMap<String, byte[]> locationSketchMap = new HashMap<>();
    for (Row row : rowList2) {
      String key = row.getString(0); // key is the location
      byte[] value = row.getAs(1); // value is the set sketch
      locationSketchMap.put(key, value);
    }

    List<Row> intersectionResults = new ArrayList<>();

    for (String location : locationSketchMap.keySet()) {
      for (String interest : interestSketchMap.keySet()) {
        byte[] b1 = interestSketchMap.get(interest);
        byte[] b2 = locationSketchMap.get(location);
        CompactSketch s1 = Sketches.heapifyCompactSketch(Memory.wrap(b1));
        CompactSketch s2 = Sketches.heapifyCompactSketch(Memory.wrap(b2));
        Intersection inter =
            Sketches.setOperationBuilder().setNominalEntries(nominalEntries).buildIntersection();
        Double estimate = inter.intersect(s1, s2).getEstimate();
        intersectionResults.add(RowFactory.create(location, interest, estimate));
      }
    }

    StructType outputSchema =
        new StructType()
            .add("location", DataTypes.StringType)
            .add("interest", DataTypes.StringType)
            .add("set_intersection_count", DataTypes.DoubleType);

    return spark.createDataFrame(intersectionResults, outputSchema);
  }

  public static void registerSetUdfs(SparkSession spark, int nominalEntries) {
    SetUnionAggregator unionAggregator = new SetUnionAggregator(nominalEntries);

    spark
        .udf()
        .register(
            SET_UNION_UDF_FUNCTION_NAME,
            org.apache.spark.sql.functions.udaf(unionAggregator, Encoders.BINARY()));
  }
}
