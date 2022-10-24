#!/bin/bash
 

# command to put in the crontab file
# 00 22 * * * export JAVA_HOME=/etc/alternatives/java_sdk_openjdk;/home/roman.le-montagner/Doctorat/GRB/Fink_GRB_test/Fink_GRB/scheduler/science2grb.sh


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

NIGHT=`date +"%Y%m%d" -d "now"`
# NIGHT=`date +"%Y%m%d" -d "now + 1 days"`
YEAR=${NIGHT:0:4}
MONTH=${NIGHT:4:2}
DAY=${NIGHT:6:2}

FINK_GRB_HOME="/home/roman.le-montagner/Doctorat/GRB/Fink_GRB_test"


# same entries as in the .conf
ZTF_ONLINE="/user/julien.peloton/online" # online_ztf_data_prefix
GCN_ONLINE="/user/roman.le-montagner/gcn_test" # online_gcn_data_prefix
ZTFXGRB_OUTPUT="/home/roman.le-montagner/ztfxgcn_storage" # online_grb_data_prefix



HDFS_HOME="/opt/hadoop-2/bin/"

while true; do

     LEASETIME=$(( `date +'%s' -d '17:00 today'` - `date +'%s' -d 'now'` ))
     echo $LEASETIME

     $(${HDFS_HOME}hdfs dfs -test -d ${ZTF_ONLINE}/science/year=${YEAR}/month=${MONTH}/day=${DAY})
     if [[ $? == 0 ]]; then
        $(${HDFS_HOME}hdfs dfs -test -d ${GCN_ONLINE}/raw/year=${YEAR}/month=${MONTH}/day=${DAY})
        if [[ $? == 0 ]]; then
            echo "Launching service"
    
            # LEASETIME must be computed by taking the difference between now and max end 
            LEASETIME=$(( `date +'%s' -d '17:00 today'` - `date +'%s' -d 'now'` ))
    
            nohup fink_grb join_stream --config ${FINK_GRB_HOME}/local.conf --night ${NIGHT} --exit_after ${LEASETIME} > ${FINK_GRB_HOME}/join_stream_${YEAR}${MONTH}${DAY}.log
            break
        fi
     fi
     if [[ $LEASETIME -le 0 ]]
     then
        echo "exit scheduler, no data for this night."
        break
     fi
     DDATE=`date`
     echo "${DDATE}: no data yet. Sleeping..."
     sleep 5
done


# Removing the _spark_metadata and grb_checkpoint directories are important. The next time the stream begins 
# will not work if these two directories exists. 

$(${HDFS_HOME}hdfs dfs -test -d ${ZTFXGRB_OUTPUT}/grb/_spark_metadata)
if [[ $? == 0 ]]; then
   echo "hdfs dfs -rm -r ${ZTFXGRB_OUTPUT}/grb/_spark_metadata"
   hdfs dfs -rm -r ${ZTFXGRB_OUTPUT}/grb/_spark_metadata
fi

$(${HDFS_HOME}hdfs dfs -test -d ${ZTFXGRB_OUTPUT}/grb_checkpoint)
if [[ $? == 0 ]]; then
   echo "hdfs dfs -rm -r ${ZTFXGRB_OUTPUT}/grb_checkpoint"
   hdfs dfs -rm -r ${ZTFXGRB_OUTPUT}/grb_checkpoint
fi

echo "Exit science2grb properly"
exit