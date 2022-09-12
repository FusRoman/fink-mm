import warnings

warnings.filterwarnings("ignore")


import pandas as pd
import numpy as np
import time
import os
import subprocess
import sys

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time
import fink_grb

from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DoubleType

from fink_broker.partitioning import convert_to_datetime
from fink_broker.sparkUtils import init_sparksession, connect_to_raw_database
from fink_grb.utils.grb_prob import p_ser_grb_vect
from fink_grb.init import get_config, init_logging
from fink_broker.science import ang2pix


@pandas_udf(DoubleType())
def grb_assoc(
    ztf_ra: pd.Series,
    ztf_dec: pd.Series,
    jdstarthist: pd.Series,
    instruments: pd.Series,
    trigger_time: pd.Series,
    grb_ra: pd.Series,
    grb_dec: pd.Series,
    grb_error: pd.Series,
    units: pd.Series,
) -> pd.Series:
    """
    Find the ztf alerts falling in the error box of the notices and emits after the trigger time.
    Then, Compute an association serendipitous probability for each of them and return it.

    Parameters
    ----------
    ztf_ra : double spark column
        right ascension coordinates of the ztf alerts
    ztf_dec : double spark column
        declination coordinates of the ztf alerts
    jdstarthist : double spark column
        Earliest Julian date of epoch corresponding to ndethist [days]
        ndethist : Number of spatially-coincident detections falling within 1.5 arcsec
            going back to beginning of survey;
            only detections that fell on the same field and readout-channel ID
            where the input candidate was observed are counted.
            All raw detections down to a photometric S/N of ~ 3 are included.
    instruments : string spark column
    trigger_time : double spark column
    grb_ra : double spark column
    grb_dec : double spark column
    grb_error : double spark column
    units : string spark column

    Returns
    grb_proba : pandas Series
        the serendipitous probability for each ztf alerts.

    Examples
    --------

    >>> sparkDF = spark.read.format('parquet').load(join_data)

    >>> df_grb = sparkDF.withColumn(
    ... "grb_proba",
    ... grb_assoc(
    ...    sparkDF.candidate.ra,
    ...     sparkDF.candidate.dec,
    ...     sparkDF.candidate.jdstarthist,
    ...     sparkDF.instruments,
    ...     sparkDF.timeUTC,
    ...     sparkDF.ra,
    ...     sparkDF.dec,
    ...     sparkDF.err,
    ...     sparkDF.units,
    ...  ),
    ... )

    >>> df_grb = df_grb.select([
    ... "objectId",
    ... "candid",
    ... col("candidate.ra").alias("ztf_ra"),
    ... col("candidate.dec").alias("ztf_dec"),
    ... "candidate.jd",
    ... "instruments",
    ... "trigger_id",
    ... col("ra").alias("grb_ra"),
    ... col("dec").alias("grb_dec"),
    ... col("err").alias("grb_loc_error"),
    ... "timeUTC",
    ... "grb_proba"
    ... ])

    >>> grb_prob = df_grb.toPandas()
    >>> grb_test = pd.read_parquet("fink_grb/test/test_data/grb_prob_test.parquet")
    >>> assert_frame_equal(grb_prob, grb_test)
    """
    grb_proba = np.ones_like(ztf_ra.values, dtype=float) * -1.0
    instruments = instruments.values

    # array of events detection rates in events/years
    # depending of the instruments
    condition = [
        np.equal(instruments, "Fermi"),
        np.equal(instruments, "SWIFT"),
        np.equal(instruments, "INTEGRAL"),
        np.equal(instruments, "ICECUBE"),
    ]
    choice_grb_rate = [250, 100, 60, 8]
    grb_det_rate = np.select(condition, choice_grb_rate)

    # array of error units depending of the instruments
    grb_error = grb_error.values
    condition = [
        grb_error == 0,
        np.equal(units, u.degree),
        np.equal(units, u.arcminute),
    ]
    conversion_units = [1 / 3600, grb_error, grb_error / 60]
    grb_error = np.select(condition, conversion_units)

    trigger_time = Time(
        pd.to_datetime(trigger_time.values, utc=True), format="datetime"
    ).jd

    # alerts emits after the grb
    delay = jdstarthist - trigger_time
    time_condition = delay > 0

    ztf_coords = SkyCoord(ztf_ra, ztf_dec, unit=u.degree)
    grb_coord = SkyCoord(grb_ra, grb_dec, unit=u.degree)

    # alerts falling within the grb_error_box
    spatial_condition = ztf_coords.separation(grb_coord).degree < 1.5 * grb_error

    # convert the delay in year
    delay_year = delay[time_condition & spatial_condition] / 365.25

    # compute serendipitous probability
    p_ser = p_ser_grb_vect(
        grb_error[time_condition & spatial_condition],
        delay_year.values,
        grb_det_rate[time_condition & spatial_condition],
    )

    grb_proba[time_condition & spatial_condition] = p_ser[0]

    return pd.Series(grb_proba)


def ztf_join_gcn_stream(
    ztf_datapath_prefix,
    gcn_datapath_prefix,
    grb_datapath_prefix,
    night,
    exit_after,
    tinterval,
    logs=False,
):
    """
    Join the ztf alerts stream and the gcn stream to find the counterparts of the gcn alerts
    in the ztf stream.

    Parameters
    ----------
    ztf_datapath_prefix : string
        the prefix path where are stored the ztf alerts.
    gcn_datapath_prefix : string
        the prefix path where are stored the gcn alerts.
    grb_datapath_prefix : string
        the prefix path to save GRB join ZTF outputs.
    night : string
        the processing night
    exit_after : int
        the maximum active time in second of the streaming process
    tinterval : int
        the processing interval time in second between the data batch

    Returns
    -------
    None

    Examples
    --------
    >>> ztf_datatest = "fink_grb/test/test_data/ztf_test/online"
    >>> gcn_datatest = "fink_grb/test/test_data/gcn_test"
    >>> grb_dataoutput = "fink_grb/test/test_output"
    >>> ztf_join_gcn_stream(
    ... ztf_datatest,
    ... gcn_datatest,
    ... grb_dataoutput,
    ... "20190903",
    ... 90,
    ... 5
    ... )

    >>> datatest = pd.read_parquet("fink_grb/test/test_data/grb_join_output.parquet")
    >>> datajoin = pd.read_parquet(grb_dataoutput + "/grb/year=2019")
    >>> assert_frame_equal(datatest, datajoin, check_dtype=False, check_column_type=False, check_categorical=False)

    >>> shutil.rmtree(grb_dataoutput + "/grb/_spark_metadata")
    >>> shutil.rmtree(grb_dataoutput + "/grb/year=2019")
    >>> shutil.rmtree(grb_dataoutput + "/grb_checkpoint")
    """
    logger = init_logging()
    spark = init_sparksession("fink_grb")

    NSIDE = 4

    scidatapath = ztf_datapath_prefix + "/science"

    # connection to the ztf science stream
    df_ztf_stream = connect_to_raw_database(
        scidatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        scidatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        latestfirst=False,
    )

    gcn_rawdatapath = gcn_datapath_prefix + "/raw"

    # connection to the gcn stream
    df_grb_stream = connect_to_raw_database(
        gcn_rawdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        gcn_rawdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        latestfirst=True,
    )

    if logs:  # pragma: no cover
        logger.info("connection to the database successfull")

    # compute healpix column for each streaming df
    df_ztf_stream = df_ztf_stream.withColumn(
        "hpix",
        ang2pix(df_ztf_stream.candidate.ra, df_ztf_stream.candidate.dec, F.lit(NSIDE)),
    )

    df_grb_stream = df_grb_stream.withColumn(
        "hpix", ang2pix(df_grb_stream.ra, df_grb_stream.dec, F.lit(NSIDE))
    )

    if logs:  # pragma: no cover
        logger.info("Healpix columns computing successfull")

    # join the two streams according to the healpix columns.
    # A pixel id will be assign to each alerts / gcn according to their position in the sky.
    # Each alerts / gcn with the same pixel id are in the same area of the sky.
    # The NSIDE correspond to a resolution of ~15 degree/pixel.
    df_grb = df_ztf_stream.join(
        df_grb_stream, df_ztf_stream["hpix"] == df_grb_stream["hpix"]
    ).drop("hpix")

    # refine the association and compute the serendipitous probability
    df_grb = df_grb.withColumn(
        "grb_proba",
        grb_assoc(
            df_grb.candidate.ra,
            df_grb.candidate.dec,
            df_grb.candidate.jdstarthist,
            df_grb.instruments,
            df_grb.timeUTC,
            df_grb.ra,
            df_grb.dec,
            df_grb.err,
            df_grb.units,
        ),
    )

    # select a subset of columns before the writing
    df_grb = df_grb.select(
        [
            "objectId",
            "candid",
            col("candidate.ra").alias("ztf_ra"),
            col("candidate.dec").alias("ztf_dec"),
            "candidate.jd",
            "instruments",
            "trigger_id",
            col("ra").alias("grb_ra"),
            col("dec").alias("grb_dec"),
            col("err").alias("grb_loc_error"),
            "timeUTC",
            "grb_proba",
        ]
    )

    # re-create partitioning columns if needed.
    timecol = "jd"
    converter = lambda x: convert_to_datetime(x)  # noqa: E731
    if "timestamp" not in df_grb.columns:
        df_grb = df_grb.withColumn("timestamp", converter(df_grb[timecol]))

    if "year" not in df_grb.columns:
        df_grb = df_grb.withColumn("year", F.date_format("timestamp", "yyyy"))

    if "month" not in df_grb.columns:
        df_grb = df_grb.withColumn("month", F.date_format("timestamp", "MM"))

    if "day" not in df_grb.columns:
        df_grb = df_grb.withColumn("day", F.date_format("timestamp", "dd"))

    grbdatapath = grb_datapath_prefix + "/grb"
    checkpointpath_grb_tmp = grb_datapath_prefix + "/grb_checkpoint"

    query_grb = (
        df_grb.writeStream.outputMode("append")
        .format("parquet")
        .option("checkpointLocation", checkpointpath_grb_tmp)
        .option("path", grbdatapath)
        .partitionBy("year", "month", "day")
        .trigger(processingTime="{} seconds".format(tinterval))
        .start()
    )

    if logs:  # pragma: no cover
        logger.info("Stream launching successfull")

    # Keep the Streaming running until something or someone ends it!
    if exit_after is not None:
        time.sleep(int(exit_after))
        query_grb.stop()
        logger.info("Exiting the science2grb streaming subprocess normally...")
    else:  # pragma: no cover
        # Wait for the end of queries
        spark.streams.awaitAnyTermination()


def launch_joining_stream(arguments):
    """
    Launch the joining stream job.

    Parameters
    ----------
    arguments : dictionnary
        arguments parse by docopt from the command line

    Returns
    -------
    None

    Examples
    --------
    >>> grb_datatest = "fink_grb/test/test_output"
    >>> gcn_datatest = "fink_grb/test/test_data/gcn_test"
    >>> launch_joining_stream({
    ... "--config" : None,
    ... "--night" : "20190903",
    ... "--exit_after" : 90
    ... })

    >>> datatest = pd.read_parquet("fink_grb/test/test_data/grb_join_output.parquet")
    >>> datajoin = pd.read_parquet(grb_datatest + "/grb/year=2019")
    >>> assert_frame_equal(datatest, datajoin, check_dtype=False, check_column_type=False, check_categorical=False)

    >>> shutil.rmtree(grb_datatest + "/grb/_spark_metadata")
    >>> shutil.rmtree(grb_datatest + "/grb/year=2019")
    >>> shutil.rmtree(grb_datatest + "/grb_checkpoint")
    """
    config = get_config(arguments)
    logger = init_logging()

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

        ztf_datapath_prefix = config["PATH"]["online_ztf_data_prefix"]
        gcn_datapath_prefix = config["PATH"]["online_gcn_data_prefix"]
        grb_datapath_prefix = config["PATH"]["online_grb_data_prefix"]
        tinterval = config["STREAM"]["tinterval"]
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

    application = os.path.join(
        os.path.dirname(fink_grb.__file__),
        "online",
        "ztf_join_gcn.py prod",
    )

    application += " " + ztf_datapath_prefix
    application += " " + gcn_datapath_prefix
    application += " " + grb_datapath_prefix
    application += " " + night
    application += " " + str(exit_after)
    application += " " + tinterval

    spark_submit = "spark-submit \
        --master {} \
        --conf spark.mesos.principal={} \
        --conf spark.mesos.secret={} \
        --conf spark.mesos.role={} \
        --conf spark.executorEnv.HOME={} \
        --driver-memory {}G \
        --executor-memory {}G \
        --conf spark.cores.max={} \
        --conf spark.executor.cores={} \
        {}".format(
        master_manager,
        principal_group,
        secret,
        role,
        executor_env,
        driver_mem,
        exec_mem,
        max_core,
        exec_core,
        application,
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
            "Fink_GRB joining stream spark application has ended with a non-zero returncode.\
                \n\t cause:\n\t\t{}\n\t\t{}".format(
                stdout, stderr
            )
        )
        exit(1)

    logger.info("Fink_GRB joining stream spark application ended normally")
    return


if __name__ == "__main__":

    if sys.argv[1] == "test":
        from fink_science.tester import spark_unit_tests
        from pandas.testing import assert_frame_equal  # noqa: F401
        import shutil  # noqa: F401

        globs = globals()

        join_data = "fink_grb/test/test_data/join_raw_datatest.parquet"
        globs["join_data"] = join_data

        # Run the test suite
        spark_unit_tests(globs)

    elif sys.argv[1] == "prod":  # pragma: no cover

        ztf_datapath_prefix = sys.argv[2]
        gcn_datapath_prefix = sys.argv[3]
        grb_datapath_prefix = sys.argv[4]
        night = sys.argv[5]
        exit_after = sys.argv[6]
        tinterval = sys.argv[7]

        ztf_join_gcn_stream(
            ztf_datapath_prefix,
            gcn_datapath_prefix,
            grb_datapath_prefix,
            night,
            exit_after,
            tinterval,
        )
