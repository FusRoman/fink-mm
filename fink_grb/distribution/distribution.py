import time
import avro.schema
import os
import subprocess
import sys

from importlib_resources import files

from fink_utils.broker.sparkUtils import init_sparksession, connect_to_raw_database
from fink_utils.broker.distributionUtils import write_to_kafka

from fink_filters.filter_on_axis_grb.filter import (
    f_bronze_events,
    f_silver_events,
    f_gold_events,
)

import fink_grb

from fink_grb.utils.fun_utils import return_verbose_level
from fink_grb.init import get_config, init_logging
from fink_grb.utils.fun_utils import build_spark_submit

import fink_filters as ff

from pyspark import SparkFiles


def grb_distribution(grbdatapath, night, tinterval, exit_after, kafka_broker_server):
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

    Return
    ------
    None

    Examples
    --------

    """
    spark = init_sparksession(
        "science2grb_offline_{}{}{}".format(night[0:4], night[4:6], night[6:8])
    )

    logger = init_logging()

    #schema_path = "fink_grb/conf/fink_grb_schema_version_1.0.avsc"
    #schema = avro.schema.parse(open(schema_path, "rb").read())
    with open(SparkFiles.get("fink_grb_schema_version_1.0.avsc"), "rb") as f:
        schema = avro.schema.parse(f.read())

    checkpointpath_grb = grbdatapath + "/grb_checkpoint"

    # connection to the grb database
    df_grb_stream = connect_to_raw_database(
        grbdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        grbdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        latestfirst=True,
    )

    df_grb_stream = df_grb_stream.drop("year").drop("month").drop("day")

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
            "f_bronze",
            f_silver_events(df_grb_stream["fink_class"], df_grb_stream["rb"]),
        )
        .filter("f_bronze == True")
        .drop("f_bronze")
    )

    df_gold = (
        df_grb_stream.withColumn(
            "f_bronze", f_gold_events(df_grb_stream["fink_class"], df_grb_stream["rb"])
        )
        .filter("f_bronze == True")
        .drop("f_bronze")
    )

    for df_filter, topicname in [
        (df_bronze, "grb_bronze_samples"),
        (df_silver, "grb_silver_samples"),
        (df_gold, "grb_gold_samples"),
    ]:

        grb_stream_distribute = write_to_kafka(
            df_filter,
            str(schema.to_json()),
            kafka_broker_server,
            "",
            "",
            topicname,
            checkpointpath_grb,
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

    print()
    print(application)
    print()

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
        spark_submit, application, external_python_libs, "", "", external_files
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
                \n\t cause:\n\t\t{}\n\t\t{}".format(
                stdout, stderr
            )
        )
        exit(1)

    logger.info("Fink_GRB distribution stream spark application ended normally")
    return


if __name__ == "__main__":

    if sys.argv[1] == "test":
        from fink_utils.test.tester import spark_unit_tests_science
        from pandas.testing import assert_frame_equal  # noqa: F401
        import shutil  # noqa: F401

        globs = globals()

        grb_data = "fink_grb/test/test_data/gcn_test/raw/year=2019/month=09/day=03"
        join_data = "fink_grb/test/test_data/join_raw_datatest.parquet"
        alert_data = "fink_grb/test/test_data/ztf_test/online/science/year=2019/month=09/day=03/ztf_science_test.parquet"
        globs["join_data"] = join_data
        globs["alert_data"] = alert_data
        globs["grb_data"] = grb_data

        # Run the test suite
        spark_unit_tests_science(globs)

    elif sys.argv[1] == "prod":  # pragma: no cover

        print()
        print(ff.__version__)
        print(sys.argv)
        print()

        grbdata_path = sys.argv[2]
        night = sys.argv[3]
        exit_after = sys.argv[4]
        tinterval = sys.argv[5]
        kafka_broker = sys.argv[6]

        grb_distribution(grbdata_path, night, tinterval, exit_after, kafka_broker)
