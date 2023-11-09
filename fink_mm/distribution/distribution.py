import time
import subprocess
import sys
import json

from fink_utils.broker.sparkUtils import init_sparksession

from fink_mm.utils.fun_utils import (
    read_and_build_spark_submit,
    read_grb_admin_options,
    read_additional_spark_options,
)
import fink_mm.utils.application as apps
from fink_mm.init import get_config, init_logging, return_verbose_level
from fink_mm.utils.fun_utils import build_spark_submit
from fink_mm.distribution.apply_filters import apply_filters

from fink_utils.spark import schema_converter
from pyspark.sql.types import StructType


def format_mangrove_col(userschema: StructType) -> StructType:
    """
    Format the mangrove column from Fink.
    The mangrove column is originally a MapType and will be cast into a Struct.

    Parameters
    ----------
    userschema : StructType
        input schema, mangrove column is a MapType

    Returns
    -------
    StructType
        ouptut schema, mangrove column is a StructType
    """
    json_schema = json.loads(userschema.json())
    mangrove_good_schema = {
        "metadata": {},
        "name": "mangrove",
        "nullable": True,
        "type": {
            "fields": [
                {
                    "metadata": {},
                    "name": "HyperLEDA_name",
                    "nullable": True,
                    "type": "string",
                },
                {
                    "metadata": {},
                    "name": "TwoMASS_name",
                    "nullable": True,
                    "type": "string",
                },
                {
                    "metadata": {},
                    "name": "lum_dist",
                    "nullable": True,
                    "type": "string",
                },
                {
                    "metadata": {},
                    "name": "ang_dist",
                    "nullable": True,
                    "type": "string",
                },
            ],
            "type": "struct",
        },
    }
    # good_mangrove_structfield = StructField.fromJson(mangrove_good_schema)
    json_schema["fields"] = [
        field for field in json_schema["fields"] if field["name"] != "mangrove"
    ]
    json_schema["fields"].append(mangrove_good_schema)
    userschema = StructType.fromJson(json_schema)
    return userschema


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
        "science2mm_distribution_{}{}{}".format(night[0:4], night[4:6], night[6:8])
    )

    logger = init_logging()

    checkpointpath_grb = grbdatapath + "/grb_distribute_checkpoint"

    grbdatapath += "/online"

    # force the mangrove columns to have the struct type
    userschema = spark.read.parquet(
        grbdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8])
    ).schema
    userschema = format_mangrove_col(userschema)

    basepath = grbdatapath + "/year={}/month={}/day={}".format(
        night[0:4], night[4:6], night[6:8]
    )
    path = basepath
    df_grb_stream = (
        spark.readStream.format("parquet")
        .schema(userschema)
        .option("basePath", basepath)
        .option("path", path)
        .option("latestFirst", True)
        .load()
    )

    # connection to the grb database
    # df_grb_stream = connect_to_raw_database(
    #     grbdatapath
    #     + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
    #     grbdatapath
    #     + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
    #     latestfirst=True,
    # )

    df_grb_stream = (
        df_grb_stream.drop("year")
        .drop("month")
        .drop("day")
        .drop("timestamp")
        .drop("t2")
        .drop("ackTime")
    )

    cnames = df_grb_stream.columns
    cnames[cnames.index("fid")] = "cast(fid as int) as fid"
    cnames[cnames.index("rb")] = "cast(rb as double) as rb"
    cnames[cnames.index("candid")] = "cast(candid as int) as candid"
    cnames[cnames.index("Plx")] = "cast(Plx as double) as Plx"
    cnames[cnames.index("e_Plx")] = "cast(e_Plx as double) as e_Plx"
    cnames[
        cnames.index("triggerTimeUTC")
    ] = "cast(triggerTimeUTC as string) as triggerTimeUTC"
    cnames[cnames.index("lc_features_g")] = "struct(lc_features_g.*) as lc_features_g"
    cnames[cnames.index("lc_features_r")] = "struct(lc_features_r.*) as lc_features_r"
    cnames[cnames.index("mangrove")] = "struct(mangrove.*) as mangrove"
    df_grb_stream = df_grb_stream.selectExpr(cnames)

    schema = schema_converter.to_avro(df_grb_stream.coalesce(1).limit(1).schema)

    grb_stream_distribute = apply_filters(
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
    ... "--config" : "fink_mm/conf/distribute_for_test.conf",
    ... "--night" : "20190903",
    ... "--exit_after" : 30,
    ... "--verbose" : False
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

    verbose = return_verbose_level(arguments, config, logger)

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

    completed_process = subprocess.run(spark_submit, shell=True, capture_output=True)

    if completed_process.returncode != 0:  # pragma: no cover
        logger.error(
            "fink-mm distribution stream spark application has ended with a non-zero returncode.\
                \n\t cause:\n\t\t{}\n\t\t{}\n\n\n{}\n\n".format(
                completed_process.stdout, completed_process.stderr, spark_submit
            )
        )
        exit(1)

    logger.info("fink-mm distribution stream spark application ended normally")
    return


if __name__ == "__main__":
    if sys.argv[1] == "prod":  # pragma: no cover
        apps.Application.DISTRIBUTION.run_application()
