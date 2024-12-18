#!/usr/bin/env bash

NUM_PEOPLE=$1
NUM_BATCHES=$2
OUTPUT_PATH_=$3
#########################################
#JAVA 17
MEM=11g
NUM_PARTITIONS="-1"
#please run 'mvn clean package' before running this script to generate the jar
JAR_PATH=target/join_and_groupby-1.0-SNAPSHOT-jar-with-dependencies.jar
#####################

echo "Generating people...${NUM_PEOPLE}"
sleep 5
java -cp $JAR_PATH:. --illegal-access=permit --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED -Dre_partitions=${NUM_PARTITIONS} -Xms${MEM} -Xmx${MEM} -XX:+UseG1GC -Duser.timezone=GMT -Djava.net.preferIPv4Stack=true org.jag.generate.PersonGenerator $OUTPUT_PATH_/people $NUM_PEOPLE $NUM_BATCHES &> generate_people.log
rm -rf $OUTPUT_PATH_/people_ || echo "okie"

echo "Generating locations for people..."
sleep 5
export NUM_PARTITIONS=-1
java -cp $JAR_PATH:. --illegal-access=permit --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED -Dre_partitions=${NUM_PARTITIONS} -Xms${MEM} -Xmx${MEM} -XX:+UseG1GC -Duser.timezone=GMT -Djava.net.preferIPv4Stack=true org.jag.generate.PersonLocationDataGenerator $OUTPUT_PATH_/people $OUTPUT_PATH_/people_location  &> generate_people_loc.log

echo "Generating interests for people..."
sleep 5
export NUM_PARTITIONS=-1
java -cp $JAR_PATH:. --illegal-access=permit --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED -Dre_partitions=${NUM_PARTITIONS} -Xms${MEM} -Xmx${MEM} -XX:+UseG1GC -Duser.timezone=GMT -Djava.net.preferIPv4Stack=true org.jag.generate.PersonInterestDataGenerator $OUTPUT_PATH_/people $OUTPUT_PATH_/people_interest  &> generate_people_interest.log

ls -lah $OUTPUT_PATH_/*