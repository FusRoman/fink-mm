import json
from fink_grb.online.ztf_join_gcn import grb_assoc
from astropy.time import Time, TimeDelta

from fink_utils.science.utils import ang2pix
from fink_utils.broker.sparkUtils import init_sparksession

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from fink_grb.init import get_config
import sys
import datetime

from fink_utils.spark.partitioning import convert_to_datetime


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


if __name__ == "__main__":

    if len(sys.argv) > 2:
        config_path = sys.argv[1]
        night = sys.argv[2]
    else:
        config_path = None
        d = datetime.datetime.today()
        night = "{}{}{}".format(d.strftime("%Y"), d.strftime("%m"), d.strftime("%d"))

    config = get_config({"--config": config_path})

    # ztf_datapath_prefix = config["PATH"]["online_ztf_data_prefix"]

    hbase_catalog = config["PATH"]["hbase_catalog"]
    gcn_datapath_prefix = config["PATH"]["online_gcn_data_prefix"]
    grb_datapath_prefix = config["PATH"]["online_grb_data_prefix"]
    time_window = int(config["OFFLINE"]["time_window"])

    spark_offline(
        hbase_catalog, gcn_datapath_prefix, grb_datapath_prefix, night, time_window
    )
