#!/usr/bin/env bash



#   --driver-java-options "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" \
#PARAM="-Dmobius.port=$1"
#if [ $# -eq 2 ]; then
#    PARAM="$PARAM -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=$2"
#fi

jars=''
for f in $(find libs/ -path '*.jar')
do
    jars=${jars}${f},
done
len=${#jars}-1
export jars=${jars:0:$len}

spark-submit \
     --class Main \
     --master spark://localhost:7077 \
     --total-executor-cores 1 \
     --executor-memory 2G \
     --driver-memory 2G \
     ./target/scala-2.11/extradatasource_2.11-1.0.jar
#     --driver-java-options "  -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005" \
#     --jars $jars \
#     "contact01_h" "select * from contact limit 30"
