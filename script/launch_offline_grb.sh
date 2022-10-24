#!/bin/bash

source ~/.bash_profile

# Other dependencies (incl. Scala part of Fink)
FINK_JARS=${FINK_HOME}/libs/fink-broker_2.11-1.2.jar,\
${FINK_HOME}/libs/hbase-spark-hbase2.2_spark3_scala2.11_hadoop2.7.jar,\
${FINK_HOME}/libs/hbase-spark-protocol-shaded-hbase2.2_spark3_scala2.11_hadoop2.7.jar

FINK_PACKAGES=org.apache.hbase:hbase-shaded-mapreduce:2.2.7

# Config 
FINK_GRB_HOME="/home/roman.le-montagner/Doctorat/GRB/Fink_GRB_test"
CONFIG=${FINK_GRB_HOME}/local.conf

NIGHT=`date +"%Y%m%d" -d "now"`
YEAR=${NIGHT:0:4}
MONTH=${NIGHT:4:2}
DAY=${NIGHT:6:2}

spark-submit \
    --master /master/ip \
    --conf spark.mesos.principal= \
    --conf spark.mesos.secret= \
    --conf spark.mesos.role= \
    --conf spark.executorEnv.HOME='/path/to/user/'\
    --driver-memory 4G --executor-memory 8G --conf spark.cores.max=16 --conf spark.executor.cores=8 \
    --jars $FINK_JARS --packages $FINK_PACKAGES \
    ${FINK_GRB_HOME}/Fink_GRB/fink_grb/offline/spark_offline.py ${CONFIG} ${NIGHT}