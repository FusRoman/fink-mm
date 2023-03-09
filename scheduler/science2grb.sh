#!/bin/bash

source ~/.bash_profile

NIGHT=`date +"%Y%m%d" -d "now"`
# NIGHT=`date +"%Y%m%d" -d "now + 1 days"`
YEAR=${NIGHT:0:4}
MONTH=${NIGHT:4:2}
DAY=${NIGHT:6:2}

FINK_GRB_HOME="/path/to/fink_grb/"


# same entries as in the .conf
ZTF_ONLINE= # online_ztf_data_prefix
GCN_ONLINE= # online_gcn_data_prefix
ZTFXGRB_OUTPUT= # online_grb_data_prefix

# pth of the hdfs installation
HDFS_HOME="/opt/hadoop-2/bin/"

while true; do

     LEASETIME=$(( `date +'%s' -d '17:00 today'` - `date +'%s' -d 'now'` ))
     echo $LEASETIME

     $(hdfs dfs -test -d ${ZTF_ONLINE}/science/year=${YEAR}/month=${MONTH}/day=${DAY})
     if [[ $? == 0 ]]; then
        $(hdfs dfs -test -d ${GCN_ONLINE}/raw/year=${YEAR}/month=${MONTH}/day=${DAY})
        if [[ $? == 0 ]]; then
            echo "Launching science2grb"
    
            # LEASETIME must be computed by taking the difference between now and max end 
            LEASETIME=$(( `date +'%s' -d '17:00 today'` - `date +'%s' -d 'now'` ))
    
            nohup fink_grb join_stream online --config ${FINK_GRB_HOME}/local.conf --night ${NIGHT} --exit_after ${LEASETIME} > ${FINK_GRB_HOME}/join_stream_${YEAR}${MONTH}${DAY}.log
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

$(hdfs dfs -test -d ${ZTFXGRB_OUTPUT}/online/_spark_metadata)
if [[ $? == 0 ]]; then
   echo "hdfs dfs -rm -r ${ZTFXGRB_OUTPUT}/online/_spark_metadata"
   hdfs dfs -rm -r ${ZTFXGRB_OUTPUT}/grb/_spark_metadata
fi

$(hdfs dfs -test -d ${ZTFXGRB_OUTPUT}/online_checkpoint)
if [[ $? == 0 ]]; then
   echo "hdfs dfs -rm -r ${ZTFXGRB_OUTPUT}/online_checkpoint"
   hdfs dfs -rm -r ${ZTFXGRB_OUTPUT}/grb_checkpoint
fi

echo "Exit science2grb properly"
exit