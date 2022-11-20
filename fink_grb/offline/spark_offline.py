import json
from fink_grb.online.ztf_join_gcn import grb_assoc
from astropy.time import Time, TimeDelta

from fink_utils.science.utils import ang2pix
from fink_utils.broker.sparkUtils import init_sparksession

from pyspark.sql import functions as F
from pyspark.sql.functions import col
import os
import sys
import subprocess

from fink_utils.spark.partitioning import convert_to_datetime

import fink_grb
from fink_grb.utils.fun_utils import return_verbose_level
from fink_grb.init import get_config, init_logging


def ztf_grb_filter(spark_ztf):

    spark_filter = (
        spark_ztf.filter(
            (spark_ztf.ssdistnr > 5)
            | (
                spark_ztf.ssdistnr == -999.0
            )  # distance to nearest known SSO above 30 arcsecond
        )
        .filter(
            (spark_ztf.distpsnr1 > 2)
            | (
                spark_ztf.ssdistnr == -999.0
            )  # distance of closest source from Pan-Starrs 1 catalog above 30 arcsecond
        )
        .filter(
            (spark_ztf.neargaia > 5)
            | (
                spark_ztf.ssdistnr == -999.0
            )  # distance of closest source from Gaia DR1 catalog above 60 arcsecond
        )
    )

    return spark_filter


def spark_offline(hbase_catalog, gcn_read_path, grbxztf_write_path, night, time_window):
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
    time_window : int
        Number of day between now and now - time_window to join ztf alerts and gcn.
        time_window are in days.

    Returns
    -------
    None
    """
    with open(hbase_catalog) as f:
        catalog = json.load(f)

    spark = init_sparksession(
        "science2grb_offline_{}{}{}".format(night[0:4], night[4:6], night[6:8])
    )

    ztf_alert = (
        spark.read.option("catalog", catalog)
        .format("org.apache.hadoop.hbase.spark")
        .option("hbase.spark.use.hbasecontext", False)
        .option("hbase.spark.pushdown.columnfilter", True)
        .load()
    )

    ztf_alert = ztf_alert.select(
        "jd_objectId",
        "objectId",
        "candid",
        "ra",
        "dec",
        "jd",
        "jdstarthist",
        "jdendhist",
        "ssdistnr",
        "distpsnr1",
        "neargaia",
    )

    now = Time.now().jd
    low_bound = now - TimeDelta(time_window * 24 * 3600, format="sec").jd

    ztf_alert = ztf_alert.filter(
        ztf_alert["jd_objectId"] >= "{}".format(low_bound)
    ).filter(ztf_alert["jd_objectId"] < "{}".format(now))

    ztf_alert = ztf_grb_filter(ztf_alert)

    ztf_alert.cache().count()

    grb_alert = spark.read.format("parquet").load(gcn_read_path)

    grb_alert = grb_alert.filter(grb_alert.triggerTimejd >= low_bound).filter(
        grb_alert.triggerTimejd <= now
    )

    grb_alert.cache().count()

    NSIDE = 4

    ztf_alert = ztf_alert.withColumn(
        "hpix",
        ang2pix(ztf_alert.ra, ztf_alert.dec, F.lit(NSIDE)),
    )

    grb_alert = grb_alert.withColumn(
        "hpix", ang2pix(grb_alert.ra, grb_alert.dec, F.lit(NSIDE))
    )

    ztf_alert = ztf_alert.withColumnRenamed("ra", "ztf_ra").withColumnRenamed(
        "dec", "ztf_dec"
    )

    grb_alert = grb_alert.withColumnRenamed("ra", "grb_ra").withColumnRenamed(
        "dec", "grb_dec"
    )

    join_condition = [
        ztf_alert.hpix == grb_alert.hpix,
        ztf_alert.jdstarthist > grb_alert.triggerTimejd,
        ztf_alert.jdendhist - grb_alert.triggerTimejd <= 10,
    ]
    join_ztf_grb = ztf_alert.join(grb_alert, join_condition, "inner")

    df_grb = join_ztf_grb.withColumn(
        "grb_proba",
        grb_assoc(
            join_ztf_grb.ztf_ra,
            join_ztf_grb.ztf_dec,
            join_ztf_grb.jdstarthist,
            join_ztf_grb.platform,
            join_ztf_grb.triggerTimeUTC,
            join_ztf_grb.grb_ra,
            join_ztf_grb.grb_dec,
            join_ztf_grb.err_arcmin,
        ),
    )

    df_grb = df_grb.select(
        [
            "objectId",
            "candid",
            "ztf_ra",
            "ztf_dec",
            "jd",
            "instrument_or_event",
            "platform",
            "triggerId",
            "grb_ra",
            "grb_dec",
            col("err_arcmin").alias("grb_loc_error"),
            "triggerTimeUTC",
            "grb_proba",
        ]
    ).filter(df_grb.grb_proba != -1.0)

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

    df_grb.write.mode("append").partitionBy("year", "month", "day").parquet(
        grbxztf_write_path
    )


def build_spark_submit(
    spark_submit, application, external_python_libs, spark_jars, packages
):
    """
    Build the spark submit command line to launch spark jobs.

    Parameters
    ----------
    spark_submit : string
        Initial spark_submit application containing the options the launch the jobs
    application : string
        The python script and their options that will be launched with the spark jobs
    external_python_libs : string
        list of external python module in .eggs format separated by ','.
    spark_jars : string
        list of external java libraries separated by ','.
    packages : string
        list of external java libraries hosted on maven, the java packages manager.
    
    Return
    ------
    spark_submit + application : string
        the initial spark_submit string with the additionnal options and libraries add to the spark_submit

    Examples
    --------
    >>> spark_submit = "spark-submit --master local[2] --driver-memory 8G --executor-memory 4G --conf spark.cores.max=4 --conf spark.executor.cores=2"
    >>> application = "myscript.py"
    >>> external_python_libs = "mypythonlibs.eggs,mypythonlibs2.py"
    >>> spark_jars = "myjavalib.jar,myjavalib2.jar"
    >>> packages = "org.apache.mylib:sublib:1.0.0"

    >>> build_spark_submit(spark_submit, application, external_python_libs, spark_jars, packages)
    'spark-submit --master local[2] --driver-memory 8G --executor-memory 4G --conf spark.cores.max=4 --conf spark.executor.cores=2 --py-files mypythonlibs.eggs,mypythonlibs2.py  --jars myjavalib.jar,myjavalib2.jar  --packages org.apache.mylib:sublib:1.0.0  myscript.py'

    >>> build_spark_submit(spark_submit, application, "", "", "")
    'spark-submit --master local[2] --driver-memory 8G --executor-memory 4G --conf spark.cores.max=4 --conf spark.executor.cores=2 myscript.py'
    """

    if application == "":
        raise ValueError("application parameters is empty !!")

    if external_python_libs != "":
        spark_submit += " --py-files {} ".format(external_python_libs)

    if spark_jars != "":
        spark_submit += " --jars {} ".format(spark_jars)

    if packages != "":
        spark_submit += " --packages {} ".format(packages)

    return spark_submit + " " + application


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

        gcn_datapath_prefix = config["PATH"]["online_gcn_data_prefix"]
        grb_datapath_prefix = config["PATH"]["online_grb_data_prefix"]
        hbase_catalog = config["PATH"]["hbase_catalog"]

        time_window = int(config["OFFLINE"]["time_window"])
    except Exception as e:  # pragma: no cover
        logger.error("Config entry not found \n\t {}".format(e))
        exit(1)

    try:
        night = arguments["--night"]
    except Exception as e:  # pragma: no cover
        logger.error("Command line arguments not found: {}\n{}".format("--night", e))
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

    application = os.path.join(
        os.path.dirname(fink_grb.__file__),
        "offline",
        "spark_offline.py prod",
    )

    application += " " + hbase_catalog
    application += " " + gcn_datapath_prefix
    application += " " + grb_datapath_prefix
    application += " " + night
    application += " " + str(time_window)

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
            ".format(
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
        spark_submit, application, external_python_libs, spark_jars, packages
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
        from fink_utils.test.tester import spark_unit_tests_science
        from pandas.testing import assert_frame_equal  # noqa: F401
        import shutil  # noqa: F401

        globs = globals()

        # join_data = "fink_grb/test/test_data/join_raw_datatest.parquet"
        # globs["join_data"] = join_data

        # Run the test suite
        spark_unit_tests_science(globs)

    if sys.argv[1] == "prod":  # pragma: no cover

        hbase_catalog = sys.argv[2]
        gcn_datapath_prefix = sys.argv[3]
        grb_datapath_prefix = sys.argv[4]
        night = sys.argv[5]
        time_window = int(sys.argv[6])

        spark_offline(
            hbase_catalog, gcn_datapath_prefix, grb_datapath_prefix, night, time_window
        )

    # if len(sys.argv) > 2:
    #     config_path = sys.argv[1]
    #     night = sys.argv[2]
    # else:
    #     config_path = None
    #     d = datetime.datetime.today()
    #     night = "{}{}{}".format(d.strftime("%Y"), d.strftime("%m"), d.strftime("%d"))

    # config = get_config({"--config": config_path})

    # # ztf_datapath_prefix = config["PATH"]["online_ztf_data_prefix"]

    # hbase_catalog = config["PATH"]["hbase_catalog"]
    # gcn_datapath_prefix = config["PATH"]["online_gcn_data_prefix"]
    # grb_datapath_prefix = config["PATH"]["online_grb_data_prefix"]
    # time_window = int(config["OFFLINE"]["time_window"])

    # spark_offline(
    #     hbase_catalog, gcn_datapath_prefix, grb_datapath_prefix, night, time_window
    # )
