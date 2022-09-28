import json
from fink_grb.online.ztf_join_gcn import ztf_grb_filter, grb_assoc
from astropy.time import Time, TimeDelta

from fink_utils.science.utils import ang2pix
from fink_utils.broker.sparkUtils import init_sparksession

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from fink_grb.init import get_config
import sys
import datetime

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


def spark_offline(gcn_read_path, grbxztf_write_path, night, time_window):
    """
    
    Parameters
    ----------

    time_window : int
        Number of day between now and now - time_window to join ztf alerts and gcn.
        time_window are in days.
    """
    path_to_catalog = "/home/julien.peloton/fink-broker/ipynb/hbase_catalogs/ztf_season1.class.json"

    with open(path_to_catalog) as f:
        catalog = json.load(f)

    spark = init_sparksession(
        "science2grb_offline_{}{}{}".format(night[0:4], night[4:6], night[6:8])
    )

    ztf_alert = spark.read.option("catalog", catalog)\
        .format("org.apache.hadoop.hbase.spark")\
        .option("hbase.spark.use.hbasecontext", False)\
        .option("hbase.spark.pushdown.columnfilter", True)\
        .load()

    ztf_alert = ztf_alert.select(
        "objectId",
        "candid",
        "ra", "dec",
        "jd", "jdstarthist",
        "class_jd_objectId",
        "ssdistnr",
        "distpsnr1",
        "neargaia"
    )

    now = Time.now().jd
    low_bound = now - TimeDelta(time_window*24*3600, format='sec').jd

    request_class = ["SN candidate","Ambiguous","Unknown", "Solar System candidate"]
    ztf_class = spark.createDataFrame([], ztf_alert.schema)

    for _class in request_class:
        ztf_class = ztf_class.union(ztf_alert\
            .filter(ztf_alert['class_jd_objectId'] >= '{}_{}'.format(_class, low_bound))\
                .filter(ztf_alert['class_jd_objectId'] < '{}_{}'.format(_class, now)))

    ztf_class.cache().count()

    ztf_class = ztf_grb_filter(ztf_class)

    grb_alert = spark.read.format("parquet").load(gcn_read_path)

    grb_alert = grb_alert\
        .filter(grb_alert.triggerTimejd >= low_bound)\
            .filter(grb_alert.triggerTimejd <= now)
    
    grb_alert.cache().count()

    NSIDE=4

    ztf_class = ztf_class.withColumn(
        "hpix",
        ang2pix(ztf_class.ra, ztf_class.dec, F.lit(NSIDE)),
    )

    grb_alert = grb_alert.withColumn(
        "hpix", ang2pix(grb_alert.ra, grb_alert.dec, F.lit(NSIDE))
    )

    ztf_class = ztf_class\
        .withColumnRenamed("ra", "ztf_ra")\
            .withColumnRenamed("dec", "ztf_dec")

    grb_alert = grb_alert\
        .withColumnRenamed("ra", "grb_ra")\
            .withColumnRenamed("dec", "grb_dec")

    join_condition = [
        ztf_class.hpix == grb_alert.hpix,
        ztf_class.jdstarthist > grb_alert.triggerTimejd,
    ]
    join_ztf_grb = ztf_class.join(grb_alert, join_condition, "inner")

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
            join_ztf_grb.err,
            join_ztf_grb.units,
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
            col("err").alias("grb_loc_error"),
            "triggerTimeUTC",
            "grb_proba",
        ]
    ).filter(df_grb.grb_proba != -1.0)

    df_grb.write.parquet(grbxztf_write_path)


if __name__ == "__main__":

    if len(sys.argv) > 2:
        config_path = sys.argv[1]
        night = sys.argv[2]
    else:
        config_path = None
        d = datetime.datetime.today()
        night = "{}{}{}".format(d.strftime('%Y'), d.strftime('%m'), d.strftime('%d'))

    config = get_config({"--config": config_path})

    # ztf_datapath_prefix = config["PATH"]["online_ztf_data_prefix"]
    gcn_datapath_prefix = config["PATH"]["online_gcn_data_prefix"]
    grb_datapath_prefix = config["PATH"]["online_grb_data_prefix"]
    time_window = config["OFFLINE"]["time_window"]

    spark_offline(gcn_datapath_prefix, grb_datapath_prefix, night, time_window)