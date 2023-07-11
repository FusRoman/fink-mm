from pyspark.sql import functions as F

from fink_utils.broker.distributionUtils import write_to_kafka
from fink_filters.filter_mm_module.filter import (
    f_grb_bronze_events,
    f_grb_silver_events,
    f_grb_gold_events,
    f_gw_bronze_events,
)


def apply_filters(
    df_stream,
    schema,
    tinterval,
    checkpointpath_grb,
    kafka_broker_server,
    username,
    password,
):
    df_grb_bronze = (
        df_stream.withColumn(
            "f_bronze",
            f_grb_bronze_events(
                df_stream["fink_class"], df_stream["observatory"], df_stream["rb"]
            ),
        )
        .filter("f_bronze == True")
        .drop("f_bronze")
    )

    df_grb_silver = (
        df_stream.withColumn(
            "f_silver",
            f_grb_silver_events(
                df_stream["fink_class"],
                df_stream["observatory"],
                df_stream["rb"],
                df_stream["grb_proba"],
            ),
        )
        .filter("f_silver == True")
        .drop("f_silver")
    )

    df_grb_gold = (
        df_stream.withColumn(
            "f_gold",
            f_grb_gold_events(
                df_stream["fink_class"],
                df_stream["observatory"],
                df_stream["rb"],
                df_stream["grb_proba"],
                df_stream["rate"],
            ),
        )
        .filter("f_gold == True")
        .drop("f_gold")
    )

    df_gw_bronze = (
        df_stream.withColumn(
            "f_bronze",
            f_gw_bronze_events(
                df_stream["fink_class"], df_stream["observatory"], df_stream["rb"]
            ),
        )
        .filter("f_bronze == True")
        .drop("f_bronze")
    )

    for df_filter, topicname in [
        (df_grb_bronze, "fink_grb_bronze"),
        (df_grb_silver, "fink_grb_silver"),
        (df_grb_gold, "fink_grb_gold"),
        (df_gw_bronze, "fink_gw_bronze"),
    ]:
        checkpointpath_topic = checkpointpath_grb + "/{}_checkpoint".format(topicname)
        grb_stream_distribute = write_to_kafka(
            df_filter,
            F.lit(schema),
            kafka_broker_server,
            username,
            password,
            topicname,
            checkpointpath_topic,
            tinterval,
        )

        return grb_stream_distribute
