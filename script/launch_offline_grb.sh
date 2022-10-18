#!/bin/bash

source ~/.bash_profile
export SPARK_HOME="/opt/spark-3/"
export PYSPARK_PYTHON="/opt/anaconda/bin/python"
export HADOOP_HOME="/opt/hadoop-2"
export HADOOP_COMMON_LIB_NATIVE_DIR="/opt/hadoop-2/lib/native"
export HADOOP_HDFS_HOME="/opt/hadoop-2"
export HADOOP_COMMON_HOME="/opt/hadoop-2"
export HADOOP_INSTALL="/opt/hadoop-2"
export HADOOP_CONF_DIR="/opt/hadoop-2/etc/hadoop"
export HADOOP_OPTS=-Djava.library.path="/opt/hadoop-2/lib/native"
export HADOOP_MAPRED_HOME="/opt/hadoop-2"

PATH=$PATH:/opt/hadoop-2/bin

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
    --master mesos://vm-75063.lal.in2p3.fr:5050 \
    --conf spark.mesos.principal=lsst \
    --conf spark.mesos.secret=secret \
    --conf spark.mesos.role=lsst \
    --conf spark.executorEnv.HOME='/home/roman.le-montagner'\
    --driver-memory 4G --executor-memory 8G --conf spark.cores.max=16 --conf spark.executor.cores=8 \
    --jars $FINK_JARS --packages $FINK_PACKAGES \
    --py-files /home/roman.le-montagner/Doctorat/GRB/Fink_GRB_test/Fink_GRB/dist/fink_grb-0.3.1-py3.7.egg \
    ${FINK_GRB_HOME}/Fink_GRB/fink_grb/offline/spark_offline.py ${CONFIG} ${NIGHT}