# join_and_groupby
Using Apache Spark - we aim to Optimise a (join of 2 datasets, followed by groupBy to get Distinct count).  
This job is a fairly common pattern in Data Engineering/Analytics.

Some people are also interested in generating HyperLogLog (HLL) instead of getting a unique count.

* Pattern 1 - join & GroupBy Get Unique Count
* Pattern 2 - join & GroupBy Generate HLL

here are Medium posts :

[Medium Post1](https://medium.com/@HumanDimension2DataEngineering/spark-optimize-100mil-x-1-bil-join-groupby-distinct-count-using-sketches-d832dc32c7a7)

[Medium Post2 ](https://medium.com/@HumanDimension2DataEngineering/spark-optimize-100mil-x-1-bil-join-groupby-generate-hll-056e66a90361)

Advantage of  HLL:
-----------------
We get to do TimeSeries Analysis!
Say we do this daily, and we want to know how many unique people visited a location, interest pair in the last 'N' days.
For each date, we can generate HLL for each location, interest pair, & then merge the last 'N' days HLL to get the unique count.

## Problem Statement
We have 2 datasets, `df1` and `df2`. We want to join them on a common key, and then group by other keys to get Distinct count.
In our case, the common is key is PersonId aka PID. 

`df1` has PID and list of Locations, and `df2` has PID and Interest.
We want to join `df1` and `df2` on PID, and then group by Location,Interest.

The aggregation to be performed is to get the count of unique PID for each Location, Interest pair.

This can also be achieved by generating a HLL (HyperLogLog) sketch for each Location, Interest pair 
or by using approx_distinct_count.

### Datasets
```
example df1

+--------+---------------------+
| pid    | locations           |
|--------|---------------------|
| John   | [Paris,Delhi,Dubai] |
| Akash  | [Delhi]             |
| Ding   | [Shanghai,Dubai]    |
+------------------------------+
example df2
+--------+----------+
| pid    | Interest |
|--------|----------|
| John   | Tea      |
| Akash  | Tea      |
| John   | Lego     |
| Akash  | Math     |
| Ding   | Gym      |
+-------------------+
```
### Expected Output
```
+--------+----------+----------------+
| Location| Interest | uniq pid #    |
|---------|----------|---------------|
| Paris   | Tea      | 1             |
| Delhi   | Tea      | 2             |
| Dubai   | Tea      | 1             |
| Delhi   | Math     | 1             |
| Paris   | Lego     | 1             |
| Delhi   | Lego     | 1             |
| Dubai   | Lego     | 1             |
| Shanghai| Gym      | 1             |
| Dubai   | Gym      | 1             |
+-------------------------------------+
```
## Pattern 1 - Get Unique Count

### Approach 1

Using spark Sql, we can join the 2 datasets, and then group by the required columns.

To get Uniq count we can use `approx distinct count` function.

```sql
WITH exploded_table AS (
    SELECT t1.pid, 
           EXPLODE(t1.locations) AS location,
           t2.interest
    FROM interests t2
    JOIN locations t1
    ON t1.pid = t2.pid
)

SELECT location, 
       interest, 
       approx_count_distinct((pid)) AS hll_pid_count
FROM exploded_table
GROUP BY location, interest
```

### Approach 2

We can use Set Operations to determine the unique Pid count for each location, interest pair.

Algorithm is as follows:

1. for each location - create a set of people who have visited that location
2. for each interest - create a set of people who have that interest
3. for each location, interest pair - find the intersection of the 2 sets, and get the count of the intersection.

For Sets, we will [Apache DataSketches library](https://datasketches.apache.org/).
```
Interest: Set Creation
-----------------------
Tea [John, Akash]
Lego [John]
Math [Akash]
Gym [Ding]

Location: Set Creation
-----------------------
Paris [John]
Delhi [John, Akash]
Shanghai [Ding]
Dubai [Ding, John]

Intersection of Sets:
----------------------
Tea ∩ Paris = 1 
Tea ∩ Delhi = 2
Tea ∩ Dubai = 1
Tea ∩ Shanghai = 0

Lego ∩ Paris = 1
Lego ∩ Delhi = 1
Lego ∩ Dubai = 1
Lego ∩ Shanghai = 0 

Math ∩ Paris = 0
Math ∩ Delhi = 1
Math ∩ Dubai = 1
Math ∩ Shanghai = 0

Gym ∩ Paris = 0
Gym ∩ Delhi = 0
Gym ∩ Dubai = 1
Gym ∩ Shanghai = 1

```

## Pattern 2 - Generate HLL

We can generate HLL for each location, interest pair. We will compare approach1/2 vs SetIntersection Method

### Approach 1

```sql
WITH exploded_table AS (
    SELECT t1.pid, 
           EXPLODE(t1.locations) AS location,
           t2.interest
    FROM interests t2
    JOIN locations t1
    ON t1.pid = t2.pid
)

SELECT location, 
       interest, 
       hll_of(pid) AS hll_bytes
```       

For the HLL library - I tried using

* [DataSketches](https://datasketches.apache.org/)
* [Aggregate Knowledge](https://github.com/aggregateknowledge/java-hll)

### Approach 2 - Using DataFrameWriter.bucketBy method to bucket the  Data

BucketBy is suggested to help with a join's performance.

```java
          locations.createOrReplaceTempView("temp_locations");
          locations = spark.sql("select pid, explode(locations) as location from temp_locations");

          locations
              .write()
              .mode(SaveMode.Overwrite)
              .format("parquet")
              .bucketBy(numBuckets, "pid")
              .option("path", outputPath + "/bucketed_table_locations")
              .saveAsTable("bucketed_table_locations");
          interests
              .write()
              .mode(SaveMode.Overwrite)
              .format("parquet")
              .bucketBy(numBuckets, "pid")
              .option("path", outputPath + "/bucketed_table_interests")
              .saveAsTable("bucketed_table_interests");
          String sql =
              "select l.location, i.interest, hll_of(i.pid) as hll_bytes from bucketed_table_interests"
                  + " i join bucketed_table_locations"
                  + " l on i.pid = l.pid group by location, interest";
          result = spark.sql(sql);
```

### Approach 3 - Physical Bucketing of Data

We will create N buckets(folders) and partition both datasets by 'pid'.

The bucketing helps by reducing pressure on the join which generally goes towards sort-merge join.
The following logs are quite familar
```text
24/12/22 04:26:44 INFO UnsafeExternalSorter: Thread 87 spilling sort data of 736.0 MiB to disk (1 times so far)
24/12/22 04:26:44 INFO UnsafeExternalSorter: Thread 87 spilling sort data of 736.0 MiB to disk (2 times so far)
```

Algorithm is as follows:

1. add column 'bucket_by_pid' = hash(pid) % N to each dataset
2. partition both datasets by 'bucket_by_pid' into 'N' Physical folders/ buckets
3. for each bucket, we will do the join and group by operation - generate HLL & save results per bucket.
4. NOTE: You can process buckets in parallel.
5. read all the results from each bucket, do a group by (interest,location) - hll_merge(hll_bytes) to get the final HLL.

```java
Dataset<Row> interests = spark.read().parquet(peopleInterestPath);
Dataset<Row> locations = spark.read().parquet(peopleLocationPath);

interests = interests.withColumn("bucket_by_pid", abs(hash(col("pid"))).mod(numBuckets));
interests
    .write()
    .partitionBy("bucket_by_pid")
    .mode(SaveMode.Overwrite)
    .parquet(outputPath + "/scrap_interests");

locations = locations.withColumn("bucket_by_pid", abs(hash(col("pid"))).mod(numBuckets));
locations
    .write()
    .partitionBy("bucket_by_pid")
    .mode(SaveMode.Overwrite)
    .parquet(outputPath + "/scrap_locations");
        
//can process buckets in parallel :)
for (int bkt = 0; bkt < numBuckets; bkt++){
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
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/staging/bucket_by_pid=" + bkt);
}

//read all results - for final merge
Dataset<Row> bucketResult = spark.read().parquet(outputPath + "/staging/*");
bucketResult.createOrReplaceTempView("bucketResults");
result =
        spark.sql(
        "SELECT location, interest, hll_merge(hll_bytes) as hll_bytes FROM bucketResults"
        + " GROUP BY location, interest");

```       

For the HLL library - I used

* [DataSketches](https://datasketches.apache.org/)
* [Aggregate Knowledge](https://github.com/aggregateknowledge/java-hll)


## Perf Test (Generation of Unique Count)

We generated people and generated for each person random locations/interests.

* Number of people = 100 million.
* Number of rows in Location Dataset = 100 million (its person vs list of locations, i.e. non-exploded table).
* Number of rows in Interest Dataset = 1Bil = 100 million * 10. (its person vs interest i.e. exploded table)

Test was conducted on Mac m1 with Xms=10g, Xmx=10g, and 8 cores - Spark Local mode.

We ran the above 2 approaches on this dataset.

### Data Generation

```bash
# generate jar
mvn clean package;
# $1=num_ppl, $2=num_batches, $3=output_dir
sh generate.sh 100000000 40 /Users/vishnuch/work/gitcode/join_and_groupby/data
```

### Running the Approaches

```bash
#Pattern 1 - Get Unique Count
# Using approach 1 - Join & GroupBy With Approx Distinct Count, $3=true i.e. use approx distinct count
sh run.sh org.jag.UsingJoinAndGroupBy /Users/vishnuch/work/gitcode/join_and_groupby/data true

# Using approach 1 - Join & GroupBy With Distinct Count, $3=false i.e. use distinct count
sh run.sh org.jag.UsingJoinAndGroupBy /Users/vishnuch/work/gitcode/join_and_groupby/data false

# Using approach 2 - Set Intersection using sketches
export NOMINAL_ENTRIES=8192
sh run.sh org.jag.UsingSetIntersection /Users/vishnuch/work/gitcode/join_and_groupby/data
```

### Results (Generation of Unique Count)
```
+-------------------------------------------+----------+--------------------------------------+
| Approach                                  | Time Sec | Notes                                |
|-------------------------------------------|----------|--------------------------------------|
| Join & GroupBy With Distinct Count        | 5797 |   | Accurate, but slower                 |
| Join & GroupBy With Approx Distinct Count |  617 |   | Faster, ~ 15 % error which is big    |
| Set Intersection using sketches           |   99 v   | Fastest,~ 3.9 % error,is tolerable!  |
+-------------------------------------------+----------+--------------------------------------+
```
Thats a **6x improvement / 83% decrease** in time taken by using Set Intersection using sketches
over Join & GroupBy With Approx Distinct Count. 😎

#### Error % with respect to Distinct Count:
```
For Join & GroupBy With Approx Distinct Count
+-----------------+---------------------------------+
| Metric          | Error % Value                   |
+-----------------+---------------------------------+
| Min Error %     | 0.005870205831513891            |
| Max Error %     | 15.15330245564193               |
| Avg Error %     | 3.761880095098103               |
|-----------------|---------------------------------|

For Set Intersection using sketches (nominal_entries=4096)
+-----------------+---------------------------------+
| Metric          | Error % Value                   |
+-----------------+---------------------------------+
| Min Error %     | 0.0013869633987065183           |
| Max Error %     | 8.578718042502715               |
| Avg Error %     | 2.252488745287771               |
|-----------------|---------------------------------|

For Set Intersection using sketches (nominal_entries=8192)
+-----------------+---------------------------------+
| Metric          | Error % Value                   |
+-----------------+---------------------------------+
| Min Error %     | 0.0031745438842705666           |
| Max Error %     | 6.212230661864266               |
| Avg Error %     | 1.4838852373872662              |
|-----------------|---------------------------------|

For Set Intersection using sketches (nominal_entries=16384)
+-----------------+---------------------------------+
| Metric          | Error % Value                   |
+-----------------+---------------------------------+
| Min Error %     | 0.00007412361104836614          |
| Max Error %     | 3.9337012309177495              |
| Avg Error %     | 0.9203915696459962              |
|-----------------|---------------------------------|

```
Effect of ['Nominal Entries'](https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html) parameter on Time Taken when using Data Sketches
```
+-------------------------------------------------+
| Nominal Entries | Time Sec | Notes              |      
|-------------------------------------------------|     |
| 4096            | 88       | max error is 8.5%  |     |
| 8192            | 89       | max error is 6.2%  |     |
| 16384           | 99       | max error is 3.9%  |     v
+-------------------------------------------------+

```

## Perf Test - select HLL (instead of approx_distinct_count)

We generated people and generated for each person random locations/interests.

* Number of people = 100 million.
* Number of rows in Location Dataset = 100 million (its person vs list of locations, i.e. non-exploded table).
* Number of rows in Interest Dataset = 100 million * 10. (its person vs interest i.e. exploded table)

Test was conducted on Mac m1 with Xms=10g, Xmx=10g, and 8 cores - Spark Local mode.

We ran the above 2 approaches on this dataset.

### Running the Approaches (Physical Bucketing vs No Bucketing)

```bash

sh generate.sh 10000000 40 /Users/vishnuch/work/gitcode/join_and_groupby/data

#Pattern 2 - Get HLL

# Using approach 1 - Join & GroupBy With HLL (no bucketing = 1 bucket)
export NUM_BUCKETS=1
export SKETCH_LIBRARY=data_sketches
export LOG_K=13
export REGISTER_WIDTH=4
sh run.sh org.jag.UsingJoinAndGroupByHLL /Users/vishnuch/work/gitcode/join_and_groupby/data

# Using approach 2 - Join & GroupBy With HLL (bucketBy Technique)
export NUM_BUCKETS=1000
export SKETCH_LIBRARY=data_sketches
export LOG_K=13
export BUCKET_TECHNIQUE=bucketby
export REGISTER_WIDTH=4
sh run.sh org.jag.UsingJoinAndGroupByHLL /Users/vishnuch/work/gitcode/join_and_groupby/data

# Using approach 3 - Join & GroupBy With HLL (physical bucketing Technique)
export NUM_BUCKETS=1000
export NUM_PARALLEL_BUCKETS=2
export SKETCH_LIBRARY=data_sketches
export LOG_K=13
export BUCKET_TECHNIQUE=physical
export REGISTER_WIDTH=4
sh run.sh org.jag.UsingJoinAndGroupByHLL /Users/vishnuch/work/gitcode/join_and_groupby/data

export NOMINAL_ENTRIES=32736
sh run.sh org.jag.UsingSetIntersection /Users/vishnuch/work/gitcode/join_and_groupby/data

```

### Results (Generation of HLL)
```
100 million people - comparing physical bucketing vs bucketBy
+----------------+--------------------------------------------------+----------+-----------------------------------------+
| HLL Library    | Approach                                         | Time Sec | Notes                                   |
|----------------+--------------------------------------------------+----------+-----------------------------------------|
| --             | SetIntersection (nominal entries = 32736)        |   130    | -                                       |
| Data Sketches  | Join & GroupBy With hll (No Bucketing)           |   7657   | -                                       |
| Data Sketches  | Join & GroupBy With hll (bucketBy 1000 Buckets)  |   7275   | Faster than No Bucketing                |
| Data Sketches  | Join & GroupBy With hll (physical 1000 Buckets)  |   6017   | 2 bkt in parallel. Faster than bucketBy |
+----------------------+-------------------------------------------+----------+------------------------------------------+

10 million people - to see effect of number of buckets on time taken & comparison with Aggregate Knowledge
+----------------------+--------------------------------------------------+----------+-----------------------------------------+
| HLL Library          | Approach                                         | Time Sec | Notes                                   |
|----------------------+--------------------------------------------------+----------+-----------------------------------------|
| Data Sketches        | Join & GroupBy With hll (No Bucketing)           |   425    | -                                       |
| Data Sketches        | Join & GroupBy With hll (physical 10 Buckets)    |   352    | 17% improvement                         |
| Data Sketches        | Join & GroupBy With hll (physical 1000 Buckets)  |   593    | Degradation! tooMany buckets.           |
| Aggregate Knowledge  | Join & GroupBy With hll (No Bucketing)           | 22514    | Most of the time is in DeSer of the Hll |
| Aggregate Knowledge  | Join & GroupBy With hll (1000 Buckets)           |  2837    | Bucketing helps to break down d join    |
+----------------------+--------------------------------------------------+----------+-----------------------------------------+

```  
NOTE: 

* We processed 2 buckets in parallel where 'N' > 1 for the above results.
* When we tried bucketBy technique with 10 buckets, we ran out of disk space , lot of data spilled onto disk > 170G ! 

```
Error % with respect to Distinct Count:

For Set Intersection using sketches (nominal_entries=32736).
For hll - we used data sketches library.

+----------------------+-----------------+---------------------------------+
| Approach             | Metric          | Error % Value                   |
+----------------------+-----------------+---------------------------------+
| Set Intersection     | Min Error %     | 0.005679614875039225            |
| Set Intersection     | Max Error %     | 3.22013506569655                |
| Set Intersection     | Avg Error %     | 0.7778856618213675              |
|   --                 |   --            |   --                            |
| Join & GroupBy HLL   | Min Error %     | 0.0009692197469855968           |
| Join & GroupBy HLL   | Max Error %     | 2.7848066786385672              |
| Join & GroupBy HLL   | Avg Error %     | 0.7741187183072222              |
+----------------------+-----------------+---------------------------------+

```

## Conclusion - Pattern 1 (generation of Unique Count)

1. Set Intersection using sketches is faster than the traditional Join & GroupBy! with tolerable error %.
2. We can tune dataSketces by changing the 'nominal entries' parameter to get the desired accuracy.
3. We achieved a **83% reduction / 6x improvement** in time taken, taking approx_distinct_count as the baseline.
4. Less Time taken = Less Cost $$ 💰$$

## Conclusion - Pattern 2 (generation of HLL)

1. Set intersection is way faster than Join & GroupBy with HLL and its error % when compared to HLL is little higher.
2. So if you can tolerate the minor error %, then Set Intersection is the way to go. HLLs are generally more accurate.
2. The join is a costly operation, and physical bucketing helps to break down the join and is faster than bucketBy method.
3. Too many buckets can lead to too many small files/spark tasks and will degrade the performance. So find the sweet spot.

