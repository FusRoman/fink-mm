import time
import os
import subprocess
import sys
import json

from fink_utils.broker.sparkUtils import init_sparksession, connect_to_raw_database
from fink_utils.broker.distributionUtils import write_to_kafka
from importlib_resources import files

from fink_filters.filter_on_axis_grb.filter import (
    f_bronze_events,
    f_silver_events,
    f_gold_events,
)

import fink_grb

from fink_grb.utils.fun_utils import return_verbose_level
from fink_grb.init import get_config, init_logging
from fink_grb.utils.fun_utils import build_spark_submit

from pyspark.sql import functions as F


def grb_distribution(
    grbdatapath, night, tinterval, exit_after, kafka_broker_server, username, password
):
    """
    Distribute the data return by the online mode over kafka.

    Parameters
    ----------
    grbdatapath: string
        path where are located the grb data produce by the online mode.
    night: string
        the processing night
        example: "20191023"
    tinterval: integer
        processing interval time between each data batch
    exit_after: int
        the maximum active time in second of the streaming process
    kafka_broker_server: string
        address of the kafka cluster
    username: string
        username for writing into the kafka cluster
    password: string
        password for writing into the kafka cluster

    Return
    ------
    None

    Examples
    --------
    >>> grb_distribution(
    ... "fink_grb/test/test_data/ztfxgcn_test/",
    ... "20230101",
    ... 30, 120,
    ... "localhost:9092",
    ... "toto", "tata"
    ... )


    >>> myconfig = {"username": "rlm", "bootstrap.servers": "localhost:9092", "group_id": "rlm_fink"}
    >>> topics = ["fink_grb_bronze"]

    >>> consumer = AlertConsumer(topics, myconfig)
    >>> topic, alert, key = consumer.poll(maxtimeout)

    >>> table = [[
    ... alert["jd"],
    ... topic,
    ... alert["objectId"],
    ... alert["fink_class"],
    ... alert["rate"]
    ... ]]

    >>> headers = [
    ... "Generated at (jd)",
    ... "Topic",
    ... "objectId",
    ... "Fink_Class",
    ... "Rate",
    ... ]

    >>> print(tabulate.tabulate(table, headers, tablefmt="pretty"))
    """
    spark = init_sparksession(
        "science2grb_distribution_{}{}{}".format(night[0:4], night[4:6], night[6:8])
    )

    logger = init_logging()

    schema_path = files("fink_grb").joinpath(
        "conf/fink_grb_schema_version_{}.avsc".format(
            fink_grb.__distribution_schema_version__
        )
    )
    with open(schema_path, "r") as f:
        schema = json.dumps(f.read())

    schema = json.loads(schema)

    checkpointpath_grb = grbdatapath + "/grb_distribute_checkpoint"

    grbdatapath += "/online"

    # connection to the grb database
    df_grb_stream = connect_to_raw_database(
        grbdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        grbdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        latestfirst=True,
    )

    df_grb_stream = (
        df_grb_stream.drop("year").drop("month").drop("day").drop("timestamp")
    )

    cnames = df_grb_stream.columns
    cnames[cnames.index("fid")] = "cast(fid as long) as fid"
    cnames[cnames.index("rb")] = "cast(rb as double) as rb"
    cnames[
        cnames.index("triggerTimeUTC")
    ] = "cast(triggerTimeUTC as string) as triggerTimeUTC"

    df_bronze = (
        df_grb_stream.withColumn(
            "f_bronze",
            f_bronze_events(df_grb_stream["fink_class"], df_grb_stream["rb"]),
        )
        .filter("f_bronze == True")
        .drop("f_bronze")
    )

    df_silver = (
        df_grb_stream.withColumn(
            "f_silver",
            f_silver_events(
                df_grb_stream["fink_class"],
                df_grb_stream["rb"],
                df_grb_stream["grb_proba"],
            ),
        )
        .filter("f_silver == True")
        .drop("f_silver")
    )

    df_gold = (
        df_grb_stream.withColumn(
            "f_gold",
            f_gold_events(
                df_grb_stream["fink_class"],
                df_grb_stream["rb"],
                df_grb_stream["grb_proba"],
                df_grb_stream["rate"],
            ),
        )
        .filter("f_gold == True")
        .drop("f_gold")
    )

    for df_filter, topicname in [
        (df_bronze, "fink_grb_bronze"),
        (df_silver, "fink_grb_silver"),
        (df_gold, "fink_grb_gold"),
    ]:

        df_filter = df_filter.selectExpr(cnames)
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

    # Keep the Streaming running until something or someone ends it!
    if exit_after is not None:
        time.sleep(int(exit_after))
        grb_stream_distribute.stop()
        logger.info("Exiting the science2grb streaming subprocess normally...")
    else:  # pragma: no cover
        # Wait for the end of queries
        spark.streams.awaitAnyTermination()


def launch_distribution(arguments):
    """


    Parameters
    ----------

    Returns
    -------
    None

    Examples
    --------

    """
    config = get_config(arguments)
    logger = init_logging()

    verbose = return_verbose_level(config, logger)

    try:
        master_manager = config["STREAM"]["manager"]
        principal_group = config["STREAM"]["principal"]
        secret = config["STREAM"]["secret"]
        role = config["STREAM"]["role"]
        executor_env = config["STREAM"]["exec_env"]
        driver_mem = config["STREAM"]["driver_memory"]
        exec_mem = config["STREAM"]["executor_memory"]
        max_core = config["STREAM"]["max_core"]
        exec_core = config["STREAM"]["executor_core"]

        grb_datapath_prefix = config["PATH"]["online_grb_data_prefix"]
        tinterval = config["STREAM"]["tinterval"]
        kafka_broker = config["DISTRIBUTION"]["kafka_broker"]
        username_writer = config["DISTRIBUTION"]["username_writer"]
        password_writer = config["DISTRIBUTION"]["password_writer"]
    except Exception as e:  # pragma: no cover
        logger.error("Config entry not found \n\t {}".format(e))
        exit(1)

    try:
        night = arguments["--night"]
    except Exception as e:  # pragma: no cover
        logger.error("Command line arguments not found: {}\n{}".format("--night", e))
        exit(1)

    try:
        exit_after = arguments["--exit_after"]
    except Exception as e:  # pragma: no cover
        logger.error(
            "Command line arguments not found: {}\n{}".format("--exit_after", e)
        )
        exit(1)

    try:
        spark_jars = config["STREAM"]["jars"]

    except Exception as e:
        if verbose:
            logger.info(
                "No spark jars dependencies specify in the following config file: {}\n\t{}".format(
                    arguments["--config"], e
                )
            )
        spark_jars = ""

    try:
        packages = config["STREAM"]["packages"]
    except Exception as e:
        if verbose:
            logger.info(
                "No packages dependencies specify in the following config file: {}\n\t{}".format(
                    arguments["--config"], e
                )
            )
        packages = ""

    try:
        external_python_libs = config["STREAM"]["external_python_libs"]
    except Exception as e:
        if verbose:
            logger.info(
                "No external python dependencies specify in the following config file: {}\n\t{}".format(
                    arguments["--config"], e
                )
            )
        external_python_libs = ""

    try:
        external_files = config["STREAM"]["external_files"]
    except Exception as e:
        if verbose:
            logger.info(
                "No external python dependencies specify in the following config file: {}\n\t{}".format(
                    arguments["--config"], e
                )
            )
        external_files = ""

    application = os.path.join(
        os.path.dirname(fink_grb.__file__),
        "distribution",
        "distribution.py prod",
    )

    application += " " + grb_datapath_prefix
    application += " " + night
    application += " " + str(exit_after)
    application += " " + tinterval
    application += " " + kafka_broker
    application += " " + username_writer
    application += " " + password_writer

    spark_submit = "spark-submit \
        --master {} \
        --conf spark.mesos.principal={} \
        --conf spark.mesos.secret={} \
        --conf spark.mesos.role={} \
        --conf spark.executorEnv.HOME={} \
        --driver-memory {}G \
        --executor-memory {}G \
        --conf spark.cores.max={} \
        --conf spark.executor.cores={}".format(
        master_manager,
        principal_group,
        secret,
        role,
        executor_env,
        driver_mem,
        exec_mem,
        max_core,
        exec_core,
    )

    spark_submit = build_spark_submit(
        spark_submit,
        application,
        external_python_libs,
        spark_jars,
        packages,
        external_files,
    )

    process = subprocess.Popen(
        spark_submit,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        shell=True,
    )

    stdout, stderr = process.communicate()
    if process.returncode != 0:  # pragma: no cover
        logger.error(
            "Fink_GRB distribution stream spark application has ended with a non-zero returncode.\
                \n\t cause:\n\t\t{}\n\t\t{}\n\n\n{}\n\n".format(
                stdout, stderr, spark_submit
            )
        )
        exit(1)

    logger.info("Fink_GRB distribution stream spark application ended normally")
    return


if __name__ == "__main__":

    if sys.argv[1] == "test":
        from fink_utils.test.tester import spark_unit_tests_broker
        from pandas.testing import assert_frame_equal  # noqa: F401
        import shutil  # noqa: F401

        globs = globals()

        # Run the test suite
        spark_unit_tests_broker(globs)

    elif sys.argv[1] == "prod":  # pragma: no cover

        grbdata_path = sys.argv[2]
        night = sys.argv[3]
        exit_after = sys.argv[4]
        tinterval = sys.argv[5]
        kafka_broker = sys.argv[6]
        username_writer = sys.argv[7]
        password_writer = sys.argv[8]

        grb_distribution(
            grbdata_path,
            night,
            tinterval,
            exit_after,
            kafka_broker,
            username_writer,
            password_writer,
        )
