#!/bin/bash

source ~/.bash_profile

# Config 
CONFIG=path/to/conf/fink_grb.conf

NIGHT=`date +"%Y%m%d" -d "now"`

nohup fink_grb join_stream offline --night=${NIGHT} --config ${CONFIG} > /path/to/logs &
