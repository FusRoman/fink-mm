import time
import subprocess
import sys
import json

from fink_utils.broker.sparkUtils import init_sparksession, connect_to_raw_database
from importlib_resources import files

import fink_grb

from fink_grb.utils.fun_utils import (
    read_and_build_spark_submit,
    read_grb_admin_options,
    read_additional_spark_options,
)
import fink_grb.utils.application as apps
from fink_grb.init import get_config, init_logging, return_verbose_level
from fink_grb.utils.fun_utils import build_spark_submit
from fink_grb.distribution.apply_filters import apply_grb_filters


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
    ... ztfxgcn_test,
    ... "20190903",
    ... 30, 40,
    ... "localhost:9092",
    ... "toto", "tata"
    ... )

    >>> consumer = AlertConsumer(topics, myconfig)
    >>> topic, alert, key = consumer.poll(maxtimeout)

    >>> table = [[
    ... alert["jd"],
    ... topic,
    ... alert["objectId"],
    ... alert["fink_class"],
    ... alert["rate"]
    ... ]]

    >>> len(table)
    1
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
    df_grb_stream = df_grb_stream.selectExpr(cnames)

    grb_stream_distribute = apply_grb_filters(
        df_grb_stream,
        schema,
        tinterval,
        checkpointpath_grb,
        kafka_broker_server,
        username,
        password,
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
    ... "--config" : "fink_grb/conf/distribute_for_test.conf",
    ... "--night" : "20190903",
    ... "--exit_after" : 30
    ... })

    >>> consumer = AlertConsumer(topics, myconfig)
    >>> topic, alert, key = consumer.poll(maxtimeout)

    >>> table = [[
    ... alert["jd"],
    ... topic,
    ... alert["objectId"],
    ... alert["fink_class"],
    ... alert["rate"]
    ... ]]

    >>> len(table)
    1
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
        _,
        kafka_broker,
        username_writer,
        password_writer,
    ) = read_grb_admin_options(arguments, config, logger)

    application = apps.Application.DISTRIBUTION.build_application(
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
    if sys.argv[1] == "prod":  # pragma: no cover
        apps.Application.DISTRIBUTION.run_application()
