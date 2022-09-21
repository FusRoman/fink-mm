#!/bin/bash
 
source ~/.bash_profile

NIGHT=`date +"%Y%m%d" -d "now + 1 days"`
YEAR=${NIGHT:0:4}
MONTH=${NIGHT:4:2}
DAY=${NIGHT:6:2}
 
FINK_GRB_HOME="/home/centos/Documents/Doctorat/GRB/Fink_GRB"

while true; do
     $(hdfs dfs -test -d /user/julien.peloton/online/science/year=${YEAR}/month=${MONTH}/day=${DAY})
     if [[ $? == 0 ]]; then
        echo "Launching service"
 
         # LEASETIME must be computed by taking the difference between now and max end 
        LEASETIME=$(( `date +'%s' -d '17:00 today'` - `date +'%s' -d 'now'` ))
 
         # nohup fink_grb join_stream --config ${FINK_GRB_HOME}/local.conf --night ${NIGHT} --exit_after ${LEASETIME} > ${FINK_GRB_HOME}/join_stream.log &
         
        ./launch_join_stream.sh $1 ${NIGHT} ${LEASETIME}
         
        exit
     fi
     DDATE=`date`
     echo "${DDATE}: no data yet. Sleeping..."
     sleep 300
 done