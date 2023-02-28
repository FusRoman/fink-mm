#!/bin/bash

source ~/.bash_profile

NIGHT=`date +"%Y%m%d" -d "now"`
# NIGHT=`date +"%Y%m%d" -d "now + 1 days"`
YEAR=${NIGHT:0:4}
MONTH=${NIGHT:4:2}
DAY=${NIGHT:6:2}

FINK_GRB_HOME="/path/to/fink_grb/"


# same entries as in the .conf
ZTFXGRB_OUTPUT= # online_grb_data_prefix

HDFS_HOME="/opt/hadoop-2/bin/"

while true; do

     LEASETIME=$(( `date +'%s' -d '17:00 today'` - `date +'%s' -d 'now'` ))
     echo $LEASETIME

     $(hdfs dfs -test -d ${ZTFXGRB_OUTPUT}/online/year=${YEAR}/month=${MONTH}/day=${DAY})
     if [[ $? == 0 ]]; then
            echo "Launching distribution"
    
            # LEASETIME must be computed by taking the difference between now and max end 
            LEASETIME=$(( `date +'%s' -d '17:00 today'` - `date +'%s' -d 'now'` ))
    
            nohup fink_grb distribute --config ${FINK_GRB_HOME}/local.conf --night ${NIGHT} --exit_after ${LEASETIME} > ${FINK_GRB_HOME}/grb_distribution_${YEAR}${MONTH}${DAY}.log
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
