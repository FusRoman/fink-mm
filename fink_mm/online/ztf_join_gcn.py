import warnings

warnings.filterwarnings("ignore")

import time
import subprocess
import sys

from pyspark.sql import functions as F
from pyspark.sql.functions import explode, col

from astropy.time import Time
from datetime import timedelta

from fink_utils.science.utils import ang2pix
from fink_utils.spark.partitioning import convert_to_datetime
from fink_utils.broker.sparkUtils import init_sparksession, connect_to_raw_database

from fink_mm.utils.fun_utils import (
    build_spark_submit,
    join_post_process,
    read_and_build_spark_submit,
    read_prior_params,
    read_additional_spark_options,
    read_grb_admin_options,
)
import fink_mm.utils.application as apps
from fink_mm.init import get_config, init_logging, return_verbose_level
from fink_mm.utils.fun_utils import get_pixels


def ztf_grb_filter(spark_ztf, ast_dist, pansstar_dist, pansstar_star_score, gaia_dist):
    """
    filter the ztf alerts by taking cross-match values from ztf.

    Parameters
    ----------
    spark_ztf : spark dataframe
        a spark dataframe containing alerts, this following columns are mandatory and have to be at the candidate level.
            - ssdistnr, distpsnr1, sgscore1, neargaia
    ast_dist: float
        distance to nearest known solar system object; set to -999.0 if none [arcsec]
        ssdistnr field
    pansstar_dist: float
        Distance of closest source from PS1 catalog; if exists within 30 arcsec [arcsec]
        distpsnr1 field
    pansstar_star_score: float
        Star/Galaxy score of closest source from PS1 catalog 0 <= sgscore <= 1 where closer to 1 implies higher likelihood of being a star
        sgscore1 field
    gaia_dist: float
        Distance to closest source from Gaia DR1 catalog irrespective of magnitude; if exists within 90 arcsec [arcsec]
        neargaia field

    Returns
    -------
    spark_filter : spark dataframe
        filtered alerts

    Examples
    --------
    >>> sparkDF = spark.read.format('parquet').load(alert_data)

    >>> spark_filter = ztf_grb_filter(sparkDF, 5, 2, 0, 5)

    >>> spark_filter.count()
    32
    """
    spark_filter = (
        spark_ztf.filter(
            (spark_ztf.candidate.ssdistnr > ast_dist)
            | (
                spark_ztf.candidate.ssdistnr == -999.0
            )  # distance to nearest known SSO above 30 arcsecond
        )
        .filter(
            (spark_ztf.candidate.distpsnr1 > pansstar_dist)
            | (
                spark_ztf.candidate.distpsnr1 == -999.0
            )  # distance of closest source from Pan-Starrs 1 catalog above 30 arcsecond
            | (spark_ztf.candidate.sgscore1 < pansstar_star_score)
        )
        .filter(
            (spark_ztf.candidate.neargaia > gaia_dist)
            | (
                spark_ztf.candidate.neargaia == -999.0
            )  # distance of closest source from Gaia DR1 catalog above 60 arcsecond
        )
    )

    return spark_filter


def check_path_exist(spark, path):
    """Check we have data for the given night on HDFS

    Parameters
    ----------
    path: str
        Path on HDFS (file or folder)

    Returns
    ----------
    out: bool
    """
    # check on hdfs
    jvm = spark._jvm
    jsc = spark._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    if fs.exists(jvm.org.apache.hadoop.fs.Path(path)):
        return True
    else:
        return False


def ztf_join_gcn_stream(
    ztf_datapath_prefix,
    gcn_datapath_prefix,
    grb_datapath_prefix,
    night,
    NSIDE,
    exit_after,
    tinterval,
    ast_dist,
    pansstar_dist,
    pansstar_star_score,
    gaia_dist,
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
    NSIDE: String
        Healpix map resolution, better if a power of 2
    exit_after : int
        the maximum active time in second of the streaming process
    tinterval : int
        the processing interval time in second between the data batch
    ast_dist: float
        distance to nearest known solar system object; set to -999.0 if none [arcsec]
        ssdistnr field
    pansstar_dist: float
        Distance of closest source from PS1 catalog; if exists within 30 arcsec [arcsec]
        distpsnr1 field
    pansstar_star_score: float
        Star/Galaxy score of closest source from PS1 catalog 0 <= sgscore <= 1 where closer to 1 implies higher likelihood of being a star
        sgscore1 field
    gaia_dist: float
        Distance to closest source from Gaia DR1 catalog irrespective of magnitude; if exists within 90 arcsec [arcsec]
        neargaia field

    Returns
    -------
    None

    Examples
    --------
    >>> grb_dataoutput_dir = tempfile.TemporaryDirectory()
    >>> grb_dataoutput = grb_dataoutput_dir.name
    >>> ztf_join_gcn_stream(
    ...     ztf_datatest,
    ...     gcn_datatest,
    ...     grb_dataoutput,
    ...     "20190903",
    ...     4, 100, 5, 5, 2, 0, 5
    ... )

    >>> datatest = pd.read_parquet(join_data_test).sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True).sort_index(axis=1)
    >>> datajoin = pd.read_parquet(grb_dataoutput + "/online").sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True).sort_index(axis=1)

    >>> datatest = datatest.drop("t2", axis=1)
    >>> datajoin = datajoin.drop("t2", axis=1)

    >>> datatest["gcn_status"] = "initial"
    >>> datatest = datatest.reindex(sorted(datatest.columns), axis=1)
    >>> datajoin = datajoin.reindex(sorted(datajoin.columns), axis=1)
    >>> assert_frame_equal(datatest, datajoin, check_dtype=False, check_column_type=False, check_categorical=False)
    """
    logger = init_logging()
    spark = init_sparksession(
        "science2mm_online_{}{}{}".format(night[0:4], night[4:6], night[6:8])
    )

    scidatapath = ztf_datapath_prefix + "/science"

    # connection to the ztf science stream
    df_ztf_stream = connect_to_raw_database(
        scidatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        scidatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        latestfirst=False,
    )
    df_ztf_stream = df_ztf_stream.select(
        "objectId",
        "candid",
        "candidate",
        "prv_candidates",
        "cdsxmatch",
        "DR3Name",
        "Plx",
        "e_Plx",
        "gcvs",
        "vsx",
        "x3hsp",
        "x4lac",
        "mangrove",
        "roid",
        "rf_snia_vs_nonia",
        "snn_snia_vs_nonia",
        "snn_sn_vs_all",
        "mulens",
        "nalerthist",
        "rf_kn_vs_nonkn",
        "t2",
        "anomaly_score",
        "lc_features_g",
        "lc_features_r",
    )

    df_ztf_stream = ztf_grb_filter(
        df_ztf_stream, ast_dist, pansstar_dist, pansstar_star_score, gaia_dist
    )

    gcn_rawdatapath = gcn_datapath_prefix

    df_grb_stream = connect_to_raw_database(
        gcn_rawdatapath,
        gcn_rawdatapath + "/year={}/month={}/day=*?*".format(night[0:4], night[4:6]),
        latestfirst=True,
    )

    # keep gcn emitted during the day time until the end of the stream (17:00 Paris Time)
    cur_time = Time(f"{night[0:4]}-{night[4:6]}-{night[6:8]}")
    last_time = cur_time - timedelta(hours=7)  # 17:00 Paris time yesterday
    end_time = cur_time + timedelta(hours=17)  # 17:00 Paris time today
    df_grb_stream = df_grb_stream.filter(
        f"triggerTimejd >= {last_time.jd} and triggerTimejd < {end_time.jd}"
    )

    if logs:  # pragma: no cover
        logger.info("connection to the database successfull")

    # compute healpix column for each streaming df

    # compute pixels for ztf alerts
    df_ztf_stream = df_ztf_stream.withColumn(
        "hpix",
        ang2pix(df_ztf_stream.candidate.ra, df_ztf_stream.candidate.dec, F.lit(NSIDE)),
    )

    # compute pixels for gcn alerts
    df_grb_stream = df_grb_stream.withColumn(
        "hpix_circle",
        get_pixels(df_grb_stream.observatory, df_grb_stream.raw_event, F.lit(NSIDE)),
    )
    df_grb_stream = df_grb_stream.withColumn("hpix", explode("hpix_circle"))

    if logs:  # pragma: no cover
        logger.info("Healpix columns computing successfull")

    df_ztf_stream = df_ztf_stream.withColumn("ztf_ra", col("candidate.ra")).withColumn(
        "ztf_dec", col("candidate.dec")
    )

    df_grb_stream = df_grb_stream.withColumnRenamed("ra", "gcn_ra").withColumnRenamed(
        "dec", "gcn_dec"
    )

    # join the two streams according to the healpix columns.
    # A pixel id will be assign to each alerts / gcn according to their position in the sky.
    # Each alerts / gcn with the same pixel id are in the same area of the sky.
    join_condition = [
        df_ztf_stream.hpix == df_grb_stream.hpix,
        df_ztf_stream.candidate.jdstarthist > df_grb_stream.triggerTimejd,
    ]
    df_grb = df_ztf_stream.join(F.broadcast(df_grb_stream), join_condition, "inner")

    df_grb = join_post_process(df_grb)

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

    grbdatapath = grb_datapath_prefix + "/online"
    checkpointpath_grb_tmp = grb_datapath_prefix + "/online_checkpoint"

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
        print("-----------------")
        logger.info(f"last progress : {query_grb.lastProgress}")
        print()
        print()
        logger.info(f"recent progress : {query_grb.recentProgress}")
        print()
        print()
        logger.info(f"query status : {query_grb.status}")
        print("-----------------")

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
    >>> launch_joining_stream({
    ...     "--config" : None,
    ...     "--night" : "20190903",
    ...     "--exit_after" : 100
    ... })

    >>> datatest = pd.read_parquet(join_data_test).sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True).sort_index(axis=1)
    >>> datajoin = pd.read_parquet("fink_mm/test/test_output/online").sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True).sort_index(axis=1)

    >>> datatest = datatest.drop("t2", axis=1)
    >>> datajoin = datajoin.drop("t2", axis=1)

    >>> datatest = datatest.reindex(sorted(datatest.columns), axis=1)
    >>> datajoin = datajoin.reindex(sorted(datajoin.columns), axis=1)
    >>> assert_frame_equal(datatest, datajoin, check_dtype=False, check_column_type=False, check_categorical=False)
    """
    config = get_config(arguments)
    logger = init_logging()

    verbose = return_verbose_level(config, logger)

    spark_submit = read_and_build_spark_submit(config, logger)

    ast_dist, pansstar_dist, pansstar_star_score, gaia_dist = read_prior_params(
        config, logger
    )

    (
        external_python_libs,
        spark_jars,
        packages,
        external_files,
    ) = read_additional_spark_options(arguments, config, logger, verbose, False)

    (
        night,
        exit_after,
        ztf_datapath_prefix,
        gcn_datapath_prefix,
        grb_datapath_prefix,
        tinterval,
        NSIDE,
        _,
        _,
        _,
        _,
        _,
    ) = read_grb_admin_options(arguments, config, logger)

    application = apps.Application.ONLINE.build_application(
        logger,
        ztf_datapath_prefix=ztf_datapath_prefix,
        gcn_datapath_prefix=gcn_datapath_prefix,
        grb_datapath_prefix=grb_datapath_prefix,
        night=night,
        NSIDE=NSIDE,
        exit_after=exit_after,
        tinterval=tinterval,
        ast_dist=ast_dist,
        pansstar_dist=pansstar_dist,
        pansstar_star_score=pansstar_star_score,
        gaia_dist=gaia_dist,
        logs=verbose,
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
            "fink-mm joining stream spark application has ended with a non-zero returncode.\
                \n\t cause:\n\t\t{}\n\t\t{}".format(
                stdout, stderr
            )
        )
        exit(1)

    logger.info("fink-mm joining stream spark application ended normally")
    return


if __name__ == "__main__":
    if sys.argv[1] == "prod":  # pragma: no cover
        apps.Application.ONLINE.run_application()
