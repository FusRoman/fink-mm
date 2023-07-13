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
    """
    Apply the user defined filters the the output of the Fink_MM package

    Parameters
    ----------
    df_stream : spark streaming dataframe
        input streaming dataframe save by the ztf_join_gcn online
    schema : dictionnary
        schema describing the data send to the kafka stream
    tinterval : integer
        interval between processing batch
    checkpointpath_grb : str
        path where are stored the kafka checkpoint
    kafka_broker_server : str
        IP adress of the kafka broker
    username : str
        username 
    password : password
        password

    Returns
    -------
    spark streaming dataframe
        same as input but filtered by the user defined filters
    """
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
                df_stream["p_assoc"],
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
                df_stream["p_assoc"],
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
