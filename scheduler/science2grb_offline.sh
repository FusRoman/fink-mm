#!/bin/bash

source ~/.bash_profile

# Config 
FINK_MM_CONFIG="path/to/conf/fink_mm.conf"
FINK_MM_LOG="path/to/store/log"

NIGHT=`date +"%Y%m%d" -d "now"`
YEAR=${NIGHT:0:4}
MONTH=${NIGHT:4:2}
DAY=${NIGHT:6:2}

nohup fink_mm join_stream offline --night=${NIGHT} --config ${FINK_MM_CONFIG} > ${FINK_MM_LOG}/fink_mm_offline_${YEAR}${MONTH}${DAY}.log &
