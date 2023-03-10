#!/bin/bash

source ~/.bash_profile

# Config 
FINK_GRB_CONFIG="path/to/conf/fink_grb.conf"
FINK_GRB_LOG="path/to/store/log"

NIGHT=`date +"%Y%m%d" -d "now"`

nohup fink_grb join_stream offline --night=${NIGHT} --config ${FINK_GRB_CONFIG} > ${FINK_GRB_LOG}/fink_grb_offline_${YEAR}${MONTH}${DAY}.log &
