import time
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

from fink_grb.utils.fun_utils import (
    return_verbose_level,
    read_and_build_spark_submit,
    read_grb_admin_options,
    read_additional_spark_options,
    Application,
)
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
    ... 30, 40,
    ... "localhost:9092",
    ... "toto", "tata"
    ... )

    >>> from fink_client.consumer import AlertConsumer
    >>> import tabulate

    >>> maxtimeout = 10
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
    +-------------------+-----------------+--------------+------------+---------------------+
    | Generated at (jd) |      Topic      |   objectId   | Fink_Class |        Rate         |
    +-------------------+-----------------+--------------+------------+---------------------+
    |  2458729.6881481  | fink_grb_bronze | ZTF19abvxuvu |  Unknown   | -1.6373900908205787 |
    +-------------------+-----------------+--------------+------------+---------------------+

    >>> shutil.rmtree("fink_grb/test/test_data/ztfxgcn_test/grb_distribute_checkpoint")
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
        logger.info("Exiting the science2grb distribution subprocess normally...")
    else:  # pragma: no cover
        # Wait for the end of queries
        spark.streams.awaitAnyTermination()


def launch_distribution(arguments):
    """
    Launch the distribution of the grb cross ztf alerts, used by the command line interface.

    Parameters
    ----------
    arguments : dictionnary
        arguments parse from the command line.

    Returns
    -------
    None

    Examples
    --------
    >>> launch_distribution({
    ... "--config" : None,
    ... "--night" : "20190903",
    ... "--exit_after" : 40
    ... })

    >>> from fink_client.consumer import AlertConsumer
    >>> import tabulate

    >>> maxtimeout = 10
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
    +-------------------+-----------------+--------------+------------+---------------------+
    | Generated at (jd) |      Topic      |   objectId   | Fink_Class |        Rate         |
    +-------------------+-----------------+--------------+------------+---------------------+
    |  2458729.6881481  | fink_grb_bronze | ZTF19abvxuvu |  Unknown   | -1.6373900908205787 |
    +-------------------+-----------------+--------------+------------+---------------------+

    >>> shutil.rmtree("fink_grb/test/test_output/grb_distribute_checkpoint")
    """
    config = get_config(arguments)
    logger = init_logging()

    verbose = return_verbose_level(config, logger)

    spark_submit = read_and_build_spark_submit(config, logger)

    (
        external_python_libs,
        spark_jars,
        packages,
        external_files,
    ) = read_additional_spark_options(arguments, config, logger, verbose, False)

    (
        night,
        exit_after,
        _,
        _,
        grb_datapath_prefix,
        tinterval,
        _,
        _,
        kafka_broker,
        username_writer,
        password_writer,
    ) = read_grb_admin_options(arguments, config, logger)

    application = Application.DISTRIBUTION.build_application(
        logger,
        grb_datapath_prefix=grb_datapath_prefix,
        night=night,
        exit_after=exit_after,
        tinterval=tinterval,
        kafka_broker=kafka_broker,
        username_writer=username_writer,
        password_writer=password_writer,
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

        Application.DISTRIBUTION.run_application()
