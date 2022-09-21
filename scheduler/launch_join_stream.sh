#!/bin/bash

source $1

spark-submit \
    --master ${manager} \
    --conf spark.mesos.principal=${principal} \
    --conf spark.mesos.secret=${secret} \
    --conf spark.mesos.role=${role} \
    --conf spark.executorEnv.HOME=${exec_env} \
    --driver-memory ${driver_memory}G \
    --executor-memory ${executor_memory}G \
    --conf spark.cores.max=${max_core} \
    --conf spark.executor.cores=${executor_core} \
    fink_grb/online/ztf_join_gcn.py "prod" \
        ${online_ztf_data_prefix} \
        ${online_gcn_data_prefix} \
        ${online_grb_data_prefix} \
        $2 $3 ${tinterval}
