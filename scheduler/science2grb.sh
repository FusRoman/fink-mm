#!/bin/bash
 
source ~/.bash_profile

# NIGHT=`date +"%Y%m%d" -d "now + 1 days"`
NIGHT=`date +"%Y%m%d" -d "now"`
YEAR=${NIGHT:0:4}
MONTH=${NIGHT:4:2}
DAY=${NIGHT:6:2}
 
FINK_GRB_HOME="/home/roman.le-montagner/Doctorat/GRB/Fink_GRB_test"

ZTF_ONLINE="/user/julien.peloton/online"
GCN_ONLINE="/user/roman.le-montagner/gcn_storage"

HDFS_HOME="/opt/hadoop-2/bin/"

while true; do
     $(${HDFS_HOME}hdfs dfs -test -d ${ZTF_ONLINE}/science/year=${YEAR}/month=${MONTH}/day=${DAY})
     if [[ $? == 0 ]]; then
        $(${HDFS_HOME}hdfs dfs -test -d ${GCN_ONLINE}/raw/year=${YEAR}/month=${MONTH}/day=${DAY})
        if [[ $? == 0 ]]; then
            echo "Launching service"
    
            # LEASETIME must be computed by taking the difference between now and max end 
            LEASETIME=$(( `date +'%s' -d '17:00 today'` - `date +'%s' -d 'now'` ))
    
            nohup fink_grb join_stream --config ${FINK_GRB_HOME}/local.conf --night ${NIGHT} --exit_after ${LEASETIME} > ${FINK_GRB_HOME}/join_stream_${YEAR}${MONTH}${DAY}.log &
            exit
        fi
     fi
     DDATE=`date`
     echo "${DDATE}: no data yet. Sleeping..."
     sleep 300
 done