import warnings

warnings.filterwarnings("ignore")

import time
import subprocess
from typing import Tuple
import sys
import json
import pandas as pd
from threading import Timer

from pyspark.sql import functions as F
from pyspark.sql.functions import explode, col, pandas_udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession, DataFrame


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
from fink_mm.init import LoggerNewLine
import fink_mm.utils.application as apps
from fink_mm.utils.fun_utils import DataMode
from fink_mm.init import get_config, init_logging, return_verbose_level
from fink_mm.utils.fun_utils import get_pixels

from fink_filters.filter_mm_module.filter import (
    f_grb_bronze_events,
    f_grb_silver_events,
    f_grb_gold_events,
    f_gw_bronze_events,
)


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


def aux_remove_skymap(d: dict) -> dict:
    """
    Remove the skymap key from the gw event given in input.

    Parameters
    ----------
    d : dict
        gw event dictionnary

    Returns
    -------
    dict
        same as input but without the skymap key.
    """
    return {
        k: v if k != "event" else {k2: v2 for k2, v2 in v.items() if k2 != "skymap"}
        for k, v in d.items()
    }


@pandas_udf(StringType())
def remove_skymap(obsname: pd.Series, rawEvent: pd.Series) -> pd.Series:
    """
    Remove the skymap key for the LVK alert

    Parameters
    ----------
    obsname : pd.Series
        observatory name
    rawEvent : pd.Series
        raw_event

    Returns
    -------
    pd.Series
        raw_event columns but for the LVK alerts, the skymap key has been removed.
    """
    return pd.Series(
        [
            json.dumps(aux_remove_skymap(json.loads(raw))) if obs == "LVK" else raw
            for obs, raw in zip(obsname, rawEvent)
        ]
    )


def load_dataframe(
    spark: SparkSession,
    ztf_path: str,
    gcn_path: str,
    night: str,
    time_window: int,
    load_mode: DataMode,
) -> Tuple[DataFrame, DataFrame]:
    if load_mode == DataMode.STREAMING:
        # connection to the ztf science stream
        ztf_alert = connect_to_raw_database(
            ztf_path
            + "/online/science/year={}/month={}/day={}".format(
                night[0:4], night[4:6], night[6:8]
            ),
            ztf_path
            + "/online/science/year={}/month={}/day={}".format(
                night[0:4], night[4:6], night[6:8]
            ),
            latestfirst=False,
        )

        userschema = spark.read.option("mergeSchema", True).parquet(gcn_path).schema
        gcn_alert = (
            spark.readStream.format("parquet")
            .schema(userschema)
            .option("basePath", gcn_path)
            .option(
                "path",
                gcn_path + "/year={}/month={}/day=*?*".format(night[0:4], night[4:6]),
            )
            .option("latestFirst", True)
            .option("mergeSchema", True)
            .load()
        )
        # keep gcn emitted during the day time until the end of the stream (17:00 Paris Time)
        cur_time = Time(f"{night[0:4]}-{night[4:6]}-{night[6:8]}")
        last_time = cur_time - timedelta(hours=7)  # 17:00 Paris time yesterday
        end_time = cur_time + timedelta(hours=17)  # 17:00 Paris time today

    elif load_mode == DataMode.OFFLINE:
        ztf_alert = (
            spark.read.format("parquet")
            .option("mergeSchema", True)
            .load(
                ztf_path
                + "/archive/science/year={}/month={}/day={}".format(
                    night[0:4], night[4:6], night[6:8]
                )
            )
        )

        gcn_alert = (
            spark.read.format("parquet").option("mergeSchema", True).load(gcn_path)
        )
        cur_time = Time(f"{night[0:4]}-{night[4:6]}-{night[6:8]}")
        last_time = cur_time - timedelta(
            days=time_window, hours=7
        )  # 17:00 Paris time yesterday
        end_time = cur_time + timedelta(hours=18)  # 18:00 Paris time today

    gcn_alert = gcn_alert.filter(
        f"triggerTimejd >= {last_time.jd} and triggerTimejd < {end_time.jd}"
    )
    return ztf_alert, gcn_alert


def write_dataframe(
    spark: SparkSession,
    df_join: DataFrame,
    write_path: str,
    logger: LoggerNewLine,
    tinterval: int,
    exit_after: int,
    logs: bool,
    test: bool,
    write_mode: DataMode,
):
    if write_mode == DataMode.STREAMING:
        grbdatapath = write_path + "/online"
        checkpointpath_grb_tmp = write_path + "/online_checkpoint"

        query_grb = (
            df_join.writeStream.outputMode("append")
            .format("parquet")
            .option("checkpointLocation", checkpointpath_grb_tmp)
            .option("path", grbdatapath)
            .partitionBy("year", "month", "day")
            .trigger(processingTime="{} seconds".format(tinterval))
            .start()
        )
        logger.info("Stream launching successfull")

        class RepeatTimer(Timer):
            def run(self):
                while not self.finished.wait(self.interval):
                    self.function(*self.args, **self.kwargs)

        def print_logs():
            if logs and not test:  # pragma: no cover
                logger.newline()
                logger.info(f"last progress : {query_grb.lastProgress}")
                logger.newline(2)
                logger.info(f"recent progress : {query_grb.recentProgress}")
                logger.newline(2)
                logger.info(f"query status : {query_grb.status}")
                logger.newline()

        logs_thread = RepeatTimer(int(tinterval) / 2, print_logs)
        logs_thread.start()
        # Keep the Streaming running until something or someone ends it!
        if exit_after is not None:
            time.sleep(int(exit_after))
            query_grb.stop()
            logs_thread.cancel()
            logger.info("Exiting the science2grb streaming subprocess normally...")
            return
        else:  # pragma: no cover
            # Wait for the end of queries
            spark.streams.awaitAnyTermination()
            return

    elif write_mode == DataMode.OFFLINE:
        df_join = df_join.withColumn(
            "is_grb_bronze",
            f_grb_bronze_events(
                df_join["fink_class"], df_join["observatory"], df_join["rb"]
            ),
        )

        df_join = df_join.withColumn(
            "is_grb_silver",
            f_grb_silver_events(
                df_join["fink_class"],
                df_join["observatory"],
                df_join["rb"],
                df_join["p_assoc"],
            ),
        )

        df_join = df_join.withColumn(
            "is_grb_gold",
            f_grb_gold_events(
                df_join["fink_class"],
                df_join["observatory"],
                df_join["rb"],
                df_join["gcn_loc_error"],
                df_join["p_assoc"],
                df_join["rate"],
            ),
        )

        df_join = df_join.withColumn(
            "is_gw_bronze",
            f_gw_bronze_events(
                df_join["fink_class"], df_join["observatory"], df_join["rb"]
            ),
        )

        grbxztf_write_path = write_path + "/offline"

        df_join.write.mode("append").partitionBy("year", "month", "day").parquet(
            grbxztf_write_path
        )
        return


def ztf_pre_join(
    ztf_dataframe: DataFrame,
    ast_dist: float,
    pansstar_dist: float,
    pansstar_star_score: float,
    gaia_dist: float,
    NSIDE: int,
) -> DataFrame:
    ztf_dataframe = ztf_dataframe.drop(
        "candid",
        "schemavsn",
        "publisher",
        "cutoutScience",
        "cutoutTemplate",
        "cutoutDifference",
        "year",
        "month",
        "day"
    )

    ztf_dataframe = ztf_grb_filter(
        ztf_dataframe, ast_dist, pansstar_dist, pansstar_star_score, gaia_dist
    )

    # compute pixels for ztf alerts
    ztf_dataframe = ztf_dataframe.withColumn(
        "hpix",
        ang2pix(ztf_dataframe.candidate.ra, ztf_dataframe.candidate.dec, F.lit(NSIDE)),
    )

    ztf_dataframe = ztf_dataframe.withColumn("ztf_ra", col("candidate.ra")).withColumn(
        "ztf_dec", col("candidate.dec")
    )
    return ztf_dataframe


def gcn_pre_join(
    gcn_dataframe: DataFrame,
    NSIDE: int,
    test: bool,
) -> DataFrame:
    gcn_dataframe = gcn_dataframe.drop("year").drop("month").drop("day")

    # compute pixels for gcn alerts
    gcn_dataframe = gcn_dataframe.withColumn(
        "hpix_circle",
        get_pixels(gcn_dataframe.observatory, gcn_dataframe.raw_event, F.lit(NSIDE)),
    )

    if not test:
        # remove the gw skymap to save memory before the join
        gcn_dataframe = gcn_dataframe.withColumn(
            "raw_event",
            remove_skymap(gcn_dataframe.observatory, gcn_dataframe.raw_event),
        )

    gcn_dataframe = gcn_dataframe.withColumn("hpix", explode("hpix_circle"))

    gcn_dataframe = gcn_dataframe.withColumnRenamed("ra", "gcn_ra").withColumnRenamed(
        "dec", "gcn_dec"
    )
    return gcn_dataframe


def ztf_join_gcn(
    mm_mode: DataMode,
    ztf_datapath_prefix: str,
    gcn_datapath_prefix: str,
    join_datapath_prefix: str,
    night: str,
    NSIDE: int,
    exit_after: int,
    tinterval: int,
    time_window: int,
    hdfs_adress: str,
    ast_dist: float,
    pansstar_dist: float,
    pansstar_star_score: float,
    gaia_dist: float,
    logs: bool = False,
    test: bool = False,
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
    join_datapath_prefix : string
        the prefix path to save GRB join ZTF outputs.
    night : string
        the processing night
    NSIDE: String
        Healpix map resolution, better if a power of 2
    exit_after : int
        the maximum active time in second of the streaming process
    tinterval : int
        the processing interval time in second between the data batch
    hdfs_adress: string
        HDFS adress used to instanciate the hdfs client from the hdfs package
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
    >>> ztf_join_gcn(
    ...     DataMode.STREAMING,
    ...     ztf_datatest,
    ...     gcn_datatest,
    ...     grb_dataoutput,
    ...     "20190903",
    ...     4, 100, 5, 7, "127.0.0.1", 5, 2, 0, 5, False, True
    ... )

    >>> datatest = pd.read_parquet(online_data_test).sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True).sort_index(axis=1)
    >>> datajoin = pd.read_parquet(grb_dataoutput + "/online").sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True).sort_index(axis=1)

    >>> datatest = datatest.drop("t2", axis=1)
    >>> datajoin = datajoin.drop("t2", axis=1)

    >>> datatest["gcn_status"] = "initial"
    >>> datatest = datatest.reindex(sorted(datatest.columns), axis=1)
    >>> datajoin = datajoin.reindex(sorted(datajoin.columns), axis=1)

    >>> list(datatest.columns) == list(datajoin.columns)
    True
    >>> len(datatest) == len(datajoin)
    True

    >>> ztf_join_gcn(
    ...     DataMode.OFFLINE,
    ...     ztf_datatest,
    ...     gcn_datatest,
    ...     grb_dataoutput,
    ...     "20190903",
    ...     4, 100, 5, 7, "127.0.0.1", 5, 2, 0, 5, False, True
    ... )

    >>> datatest = pd.read_parquet(offline_data_test).sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True).sort_index(axis=1)
    >>> datajoin = pd.read_parquet(grb_dataoutput + "/offline").sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True).sort_index(axis=1)

    >>> datatest = datatest.drop("t2", axis=1)
    >>> datajoin = datajoin.drop("t2", axis=1)

    >>> datatest["gcn_status"] = "initial"
    >>> datatest = datatest.reindex(sorted(datatest.columns), axis=1)
    >>> datajoin = datajoin.reindex(sorted(datajoin.columns), axis=1)

    >>> list(datatest.columns) == list(datajoin.columns)
    True
    >>> len(datatest) == len(datajoin)
    True
    """
    logger = init_logging()

    if mm_mode == DataMode.OFFLINE:
        job_name = "offline"
    elif mm_mode == DataMode.STREAMING:
        job_name = "online"

    spark = init_sparksession(
        "science2mm_{}_{}{}{}".format(job_name, night[0:4], night[4:6], night[6:8])
    )

    ztf_dataframe, gcn_dataframe = load_dataframe(
        spark,
        ztf_datapath_prefix,
        gcn_datapath_prefix,
        night,
        int(time_window),
        mm_mode,
    )
    ztf_dataframe = ztf_pre_join(
        ztf_dataframe, ast_dist, pansstar_dist, pansstar_star_score, gaia_dist, NSIDE
    )
    gcn_dataframe = gcn_pre_join(gcn_dataframe, NSIDE, test)

    # join the two streams according to the healpix columns.
    # A pixel id will be assign to each alerts / gcn according to their position in the sky.
    # Each alerts / gcn with the same pixel id are in the same area of the sky.
    join_condition = [
        ztf_dataframe.hpix == gcn_dataframe.hpix,
        ztf_dataframe.candidate.jdstarthist > gcn_dataframe.triggerTimejd,
    ]
    df_join_mm = gcn_dataframe.join(F.broadcast(ztf_dataframe), join_condition, "inner")

    df_join_mm = join_post_process(df_join_mm, hdfs_adress, gcn_datapath_prefix)

    # re-create partitioning columns if needed.
    timecol = "jd"
    converter = lambda x: convert_to_datetime(x)  # noqa: E731
    if "timestamp" not in df_join_mm.columns:
        df_join_mm = df_join_mm.withColumn("timestamp", converter(df_join_mm[timecol]))

    if "year" not in df_join_mm.columns:
        df_join_mm = df_join_mm.withColumn("year", F.date_format("timestamp", "yyyy"))

    if "month" not in df_join_mm.columns:
        df_join_mm = df_join_mm.withColumn("month", F.date_format("timestamp", "MM"))

    if "day" not in df_join_mm.columns:
        df_join_mm = df_join_mm.withColumn("day", F.date_format("timestamp", "dd"))

    write_dataframe(
        spark,
        df_join_mm,
        join_datapath_prefix,
        logger,
        tinterval,
        exit_after,
        logs,
        test,
        mm_mode,
    )


def launch_join(arguments: dict, data_mode, test: bool = False):
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
    >>> launch_join({
    ...     "--config" : None,
    ...     "--night" : "20190903",
    ...     "--exit_after" : 100,
    ...     "--verbose" : False
    ... }, DataMode.STREAMING, True)

    >>> datatest = pd.read_parquet(online_data_test).sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True).sort_index(axis=1)
    >>> datajoin = pd.read_parquet("fink_mm/test/test_output/online").sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True).sort_index(axis=1)

    >>> datatest = datatest.drop("t2", axis=1)
    >>> datajoin = datajoin.drop("t2", axis=1)

    >>> datatest["gcn_status"] = "initial"
    >>> datatest = datatest.reindex(sorted(datatest.columns), axis=1)
    >>> datajoin = datajoin.reindex(sorted(datajoin.columns), axis=1)

    >>> list(datatest.columns) == list(datajoin.columns)
    True
    >>> len(datatest) == len(datajoin)
    True

    >>> launch_join({
    ...     "--config" : None,
    ...     "--night" : "20190903",
    ...     "--exit_after" : 100,
    ...     "--verbose" : False
    ... }, DataMode.OFFLINE, True)

    >>> datatest = pd.read_parquet(offline_data_test).sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True).sort_index(axis=1)
    >>> datajoin = pd.read_parquet("fink_mm/test/test_output/offline").sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True).sort_index(axis=1)

    >>> datatest = datatest.drop("t2", axis=1)
    >>> datajoin = datajoin.drop("t2", axis=1)

    >>> datatest["gcn_status"] = "initial"
    >>> datatest = datatest.reindex(sorted(datatest.columns), axis=1)
    >>> datajoin = datajoin.reindex(sorted(datajoin.columns), axis=1)

    >>> list(datatest.columns) == list(datajoin.columns)
    True
    >>> len(datatest) == len(datajoin)
    True
    """
    config = get_config(arguments)
    logger = init_logging()

    verbose, debug = return_verbose_level(arguments, config, logger)

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
        hdfs_adress,
        NSIDE,
        _,
        time_window,
        _,
        _,
        _,
    ) = read_grb_admin_options(arguments, config, logger)

    application = apps.Application.JOIN.build_application(
        logger,
        data_mode=data_mode,
        ztf_datapath_prefix=ztf_datapath_prefix,
        gcn_datapath_prefix=gcn_datapath_prefix,
        grb_datapath_prefix=grb_datapath_prefix,
        night=night,
        NSIDE=NSIDE,
        exit_after=exit_after,
        tinterval=tinterval,
        time_window=time_window,
        ast_dist=ast_dist,
        pansstar_dist=pansstar_dist,
        pansstar_star_score=pansstar_star_score,
        gaia_dist=gaia_dist,
        logs=verbose,
        hdfs_adress=hdfs_adress,
        is_test=test,
    )

    if debug:
        logger.debug(f"application command = {application}")

    spark_submit = build_spark_submit(
        spark_submit,
        application,
        external_python_libs,
        spark_jars,
        packages,
        external_files,
    )

    if debug:
        logger.debug(f"spark-submit command = {spark_submit}")

    completed_process = subprocess.run(spark_submit, shell=True, capture_output=True)

    if completed_process.returncode != 0:  # pragma: no cover
        logger.error(
            f"fink-mm joining stream spark application has ended with a non-zero returncode.\
                \n\tstdout:\n\n{completed_process.stdout} \n\tstderr:\n\n{completed_process.stderr}"
        )
        exit(1)

    if arguments["--verbose"]:
        logger.info("fink-mm joining stream spark application ended normally")
        print()
        logger.info(f"job logs:\n\n{completed_process.stdout}")
    return


if __name__ == "__main__":
    if sys.argv[1] == "streaming":  # pragma: no cover
        apps.Application.JOIN.run_application(DataMode.STREAMING)
    elif sys.argv[1] == "offline":
        apps.Application.JOIN.run_application(DataMode.OFFLINE)
