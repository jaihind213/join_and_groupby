# join_and_groupby
Using Apache Spark - we aim to Optimise a (join of 2 datasets, followed by groupBy).  
This job is a fairly common pattern in Data Engineering/Analytics.

## Problem Statement
We have 2 datasets, `df1` and `df2`. We want to join them on a common key, and then group by other keys.
In our case, the common is key is PersonId aka PID. 

`df1` has PID and list of Locations, and `df2` has PID and Interest.
We want to join `df1` and `df2` on PID, and then group by Location,Interest.

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
## Approaches

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

## Perf Test

We generated people and generated for each person random locations/interests.

* Number of people = 100 million.
* Number of rows in Location Dataset = 100 million.
* Number of rows in Interest Dataset = 1000 million.

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
# Using approach 1 - Join & GroupBy With Distinct Count, $3=true i.e. use approx distinct count
sh run.sh org.jag.UsingJoinAndGroupBy /Users/vishnuch/work/gitcode/join_and_groupby/data true`

# Using approach 1 - Join & GroupBy With Distinct Count, $3=false i.e. use distinct count
sh run.sh org.jag.UsingSetIntersection /Users/vishnuch/work/gitcode/join_and_groupby/data false

# Using approach 2 - Set Intersection using sketches
export NOMINAL_ENTRIES=8192
sh run.sh org.jag.UsingSetIntersection /Users/vishnuch/work/gitcode/join_and_groupby/data
```

### Results
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

## Conclusion

1. Set Intersection using sketches is faster than the traditional Join & GroupBy! with tolerable error %.
2. We can tune dataSketces by changing the 'nominal entries' parameter to get the desired accuracy.
2. We achieved a **83% reduction / 6x improvement** in time taken, taking approx_distinct_count as the baseline.
3. Less Time taken = Less Cost $$ 💰$$