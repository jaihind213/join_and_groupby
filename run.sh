#!/usr/bin/env bash

KLASS=$1
DATA_PATH=$2
USE_APPROX_DISTINCT_COUNT=$3

if [ "$USE_APPROX_DISTINCT_COUNT" == "" ]; then
  #only used by joinAndGroupby Algo
  USE_APPROX_DISTINCT_COUNT="true"
fi

if [ "$NOMINAL_ENTRIES" == "" ]; then
  #only used by setIntersection Algo
  NOMINAL_ENTRIES="16384"
fi
echo "using NOMINAL_ENTRIES=$NOMINAL_ENTRIES"

if [ "$NUM_BUCKETS" == "" ]; then
  export NUM_BUCKETS=1
fi
echo "using NUM_BUCKETS=$NUM_BUCKETS"

if [ "$NUM_PARALLEL_BUCKETS" == "" ]; then
  #only used by hll job
  export NUM_PARALLEL_BUCKETS=2
fi
echo "using NUM_PARALLEL_BUCKETS=$NUM_PARALLEL_BUCKETS"

if [ "$SKETCH_LIBRARY" == "" ]; then
  #only used by hll job
  export SKETCH_LIBRARY=data_sketches
fi
echo "using SKETCH_LIBRARY=$SKETCH_LIBRARY"

if [ "$LOG_K" == "" ]; then
  #only used by hll job
  export LOG_K=13
fi
echo "using LOG_K=$LOG_K"

if [ "$BUCKET_TECHNIQUE" == "" ]; then
  #only used by hll job
  export BUCKET_TECHNIQUE=physical
fi
echo "using BUCKET_TECHNIQUE=$BUCKET_TECHNIQUE"

if [ "$REGISTER_WIDTH" == "" ]; then
  #only used by hll job
  export REGISTER_WIDTH=4
fi
echo "using REGISTER_WIDTH=$REGISTER_WIDTH"


#JAVA 17
####################
#DATA_PATH=/Users/vishnuch/work/gitcode/bytespireio/join_and_groupby/data
PPL_LOCATION_PARQUET_PATH=$DATA_PATH/people_location
PPL_INTEREST_PARQUET_PATH=$DATA_PATH/people_interest
OUTPUT_PATH_=$DATA_PATH/results
mkdir -p $OUTPUT_PATH_
MEM=10g
JAR_PATH=target/join_and_groupby-1.0-SNAPSHOT-jar-with-dependencies.jar
#####################

rm -rf $DATA_PATH/results/staging/ || echo ""
rm -rf $DATA_PATH/results/scrap_*  || echo ""
rm -rf $DATA_PATH/results/spark-warehouse || echo ""
rm -rf ${KLASS}.out.log || echo "okie"

java -cp $JAR_PATH:. --illegal-access=permit --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED -Xms${MEM} -Xmx${MEM} -XX:+UseG1GC -Dnominal_entries=${NOMINAL_ENTRIES} -Duser.timezone=GMT -Djava.net.preferIPv4Stack=true $KLASS $PPL_INTEREST_PARQUET_PATH $PPL_LOCATION_PARQUET_PATH $OUTPUT_PATH_ $USE_APPROX_DISTINCT_COUNT &> ${KLASS}.log
