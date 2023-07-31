import json
from astropy.time import TimeDelta, Time

from fink_utils.science.utils import ang2pix
from fink_utils.broker.sparkUtils import init_sparksession

from pyspark.sql import functions as F
from pyspark.sql.functions import explode, col
import sys
import subprocess

from fink_utils.spark.partitioning import convert_to_datetime

from fink_mm.utils.fun_utils import (
    build_spark_submit,
    join_post_process,
    read_and_build_spark_submit,
    read_prior_params,
    read_grb_admin_options,
    read_additional_spark_options,
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
            - ssdistnr, distpsnr1, neargaia

    Returns
    -------
    spark_filter : spark dataframe
        filtered alerts
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

    Examples
    --------
    >>> sparkDF = spark.read.format('parquet').load(alert_data)

    >>> sparkDF = sparkDF.select(
    ... "objectId",
    ... "candid",
    ... "candidate.ra",
    ... "candidate.dec",
    ... "candidate.jd",
    ... "candidate.jdstarthist",
    ... "candidate.jdendhist",
    ... "candidate.ssdistnr",
    ... "candidate.distpsnr1",
    ... "candidate.sgscore1",
    ... "candidate.neargaia",
    ... )

    >>> spark_filter = ztf_grb_filter(sparkDF, 5, 2, 0, 5)

    >>> spark_filter.count()
    32
    """
    spark_filter = (
        spark_ztf.filter(
            (spark_ztf.ssdistnr > ast_dist)
            | (
                spark_ztf.ssdistnr == -999.0
            )  # distance to nearest known SSO above 30 arcsecond
        )
        .filter(
            (spark_ztf.distpsnr1 > pansstar_dist)
            | (
                spark_ztf.distpsnr1 == -999.0
            )  # distance of closest source from Pan-Starrs 1 catalog above 30 arcsecond
            | (spark_ztf.sgscore1 < pansstar_star_score)
        )
        .filter(
            (spark_ztf.neargaia > gaia_dist)
            | (
                spark_ztf.neargaia == -999.0
            )  # distance of closest source from Gaia DR1 catalog above 60 arcsecond
        )
    )

    return spark_filter


def spark_offline(
    hbase_catalog: str,
    gcn_read_path: str,
    grbxztf_write_path: str,
    night: str,
    NSIDE: int,
    start_window: float,
    time_window: int,
    ast_dist: float,
    pansstar_dist: float,
    pansstar_star_score: float,
    gaia_dist: float,
    with_columns_filter: bool = True,
):
    """
    Cross-match Fink and the GNC in order to find the optical alerts falling in the error box of a GCN.

    Parameters
    ----------
    hbase_catalog : string
        path to the hbase catalog (json format)
        Key index must be jd_objectId
    gcn_read_path : string
        path to the gcn database
    grbxztf_write_path : string
        path to store the cross match ZTF/GCN results
    night : string
        launching night of the script
    NSIDE: int
        Healpix map resolution, better if a power of 2
    start_window : float
        start date of the time window (in jd / julian date)
    time_window : int
        Number of day between start_window and (start_window - time_window) to join ztf alerts and gcn.
        time_window are in days.
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
    with_columns_filter : boolean
        Hbase options to optimize loading, work only in distributed mode
        Set this option at False if in local mode. default = True

    Returns
    -------
    None

    Examples
    --------
    >>> grb_dataoutput_dir = tempfile.TemporaryDirectory()
    >>> grb_dataoutput = grb_dataoutput_dir.name

    >>> spark_offline(
    ...    hbase_catalog,
    ...    gcn_datatest,
    ...    grb_dataoutput,
    ...    "20190903",
    ...    4,
    ...    Time("2019-09-04").jd,
    ...    7, 5, 2, 0, 5,
    ...    False
    ... )

    >>> datajoin = pd.read_parquet(grb_dataoutput + "/offline").sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True)
    >>> datatest = pd.read_parquet(offline_data_test).sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True)

    >>> cols = ['t2_AGN', 't2_EB',
    ... 't2_KN', 't2_M-dwarf', 't2_Mira', 't2_RRL', 't2_SLSN-I', 't2_SNII',
    ... 't2_SNIa', 't2_SNIa-91bg', 't2_SNIax', 't2_SNIbc', 't2_TDE',
    ... 't2_mu-Lens-Single']
    >>> datatest = datatest.drop(cols, axis=1)
    >>> datajoin = datajoin.drop(cols + ["year", "month", "day"], axis=1)
    
    >>> assert_frame_equal(datatest, datajoin, check_dtype=False, check_column_type=False, check_categorical=False)
    """
    spark = init_sparksession(
        "science2mm_offline_{}{}{}".format(night[0:4], night[4:6], night[6:8])
    )
    logger = init_logging()
    low_bound = start_window - TimeDelta(time_window * 24 * 3600, format="sec").jd

    if low_bound < 0 or low_bound > start_window:
        raise ValueError(
            "The time window is higher than the start_window : \nstart_window = {}\ntime_window = {}\nlow_bound={}".format(
                start_window, time_window, low_bound
            )
        )

    grb_alert = spark.read.format("parquet").load(gcn_read_path)

    grb_alert = grb_alert.filter(grb_alert.triggerTimejd >= low_bound).filter(
        grb_alert.triggerTimejd <= start_window
    )

    nb_gcn_alert = grb_alert.cache().count()
    if nb_gcn_alert == 0:
        logger.info(
            "No gcn between {} and {}, exit the offline mode.".format(
                Time(low_bound, format="jd").iso, Time(start_window, format="jd").iso
            )
        )
        return

    with open(hbase_catalog) as f:
        catalog = json.load(f)

    ztf_alert = (
        spark.read.option("catalog", catalog)
        .format("org.apache.hadoop.hbase.spark")
        .option("hbase.spark.use.hbasecontext", False)
        .option("hbase.spark.pushdown.columnfilter", with_columns_filter)
        .load()
        .filter(~col("jd_objectId").startswith("schema_"))  # remove key column
    )

    ztf_alert = ztf_alert.filter(
        ztf_alert["jd_objectId"] >= "{}".format(low_bound)
    ).filter(ztf_alert["jd_objectId"] < "{}".format(start_window))

    ztf_alert = ztf_grb_filter(
        ztf_alert, ast_dist, pansstar_dist, pansstar_star_score, gaia_dist
    )

    ztf_alert.cache().count()

    ztf_alert = ztf_alert.withColumn(
        "hpix",
        ang2pix(ztf_alert.ra, ztf_alert.dec, F.lit(NSIDE)),
    )

    grb_alert = grb_alert.withColumn(
        "hpix_circle",
        get_pixels(grb_alert.observatory, grb_alert.raw_event, F.lit(NSIDE)),
    )
    grb_alert = grb_alert.withColumn("hpix", explode("hpix_circle"))

    ztf_alert = ztf_alert.withColumnRenamed("ra", "ztf_ra").withColumnRenamed(
        "dec", "ztf_dec"
    )

    grb_alert = grb_alert.withColumnRenamed("ra", "gcn_ra").withColumnRenamed(
        "dec", "gcn_dec"
    )

    join_condition = [
        ztf_alert.hpix == grb_alert.hpix,
        ztf_alert.jdstarthist > grb_alert.triggerTimejd,
        ztf_alert.jdendhist - grb_alert.triggerTimejd <= 10,
    ]
    join_ztf_grb = ztf_alert.join(grb_alert, join_condition, "inner")

    df_grb = join_post_process(join_ztf_grb, with_rate=False, from_hbase=True)

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

    grbxztf_write_path = grbxztf_write_path + "/offline"

    df_grb.write.mode("append").partitionBy("year", "month", "day").parquet(
        grbxztf_write_path
    )


def launch_offline_mode(arguments):
    """
    Launch the offline grb module, used by the command line interface.

    Parameters
    ----------
    arguments : dictionnary
        arguments parse from the command line.

    Returns
    -------
    None

    Examples
    --------
    >>> launch_offline_mode({
    ...         "--config" : None,
    ...         "--night" : "20190903",
    ...         "--exit_after" : 100,
    ...         "--test" : True
    ...     }
    ... )

    >>> datajoin = pd.read_parquet("fink_mm/test/test_output/offline").sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True)
    >>> datatest = pd.read_parquet(offline_data_test).sort_values(["objectId", "triggerId", "gcn_ra"]).reset_index(drop=True)

    >>> cols = ['t2_AGN', 't2_EB',
    ... 't2_KN', 't2_M-dwarf', 't2_Mira', 't2_RRL', 't2_SLSN-I', 't2_SNII',
    ... 't2_SNIa', 't2_SNIa-91bg', 't2_SNIax', 't2_SNIbc', 't2_TDE',
    ... 't2_mu-Lens-Single']
    >>> datatest = datatest.drop(cols, axis=1)
    >>> datajoin = datajoin.drop(cols + ["year", "month", "day"], axis=1)

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
    ) = read_additional_spark_options(
        arguments, config, logger, verbose, arguments["--test"]
    )

    (
        night,
        _,
        _,
        gcn_datapath_prefix,
        grb_datapath_prefix,
        _,
        NSIDE,
        hbase_catalog,
        time_window,
        _,
        _,
        _,
    ) = read_grb_admin_options(arguments, config, logger, is_test=arguments["--test"])

    application = apps.Application.OFFLINE.build_application(
        logger,
        hbase_catalog=hbase_catalog,
        gcn_datapath_prefix=gcn_datapath_prefix,
        grb_datapath_prefix=grb_datapath_prefix,
        night=night,
        NSIDE=NSIDE,
        time_window=time_window,
        ast_dist=ast_dist,
        pansstar_dist=pansstar_dist,
        pansstar_star_score=pansstar_star_score,
        gaia_dist=gaia_dist,
        is_test=arguments["--test"],
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
            "fink-mm offline crossmatch application has ended with a non-zero returncode.\
                \n\t cause:\n\t\t{}\n\t\t{}".format(
                stdout, stderr
            )
        )
        exit(1)

    logger.info("fink-mm offline crossmatch application ended normally")
    return


if __name__ == "__main__":
    if sys.argv[1] == "prod":  # pragma: no cover
        apps.Application.OFFLINE.run_application()
