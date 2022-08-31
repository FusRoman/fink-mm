import pandas as pd
import numpy as np
import time

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time

from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

from fink_broker.sparkUtils import init_sparksession, connect_to_raw_database
from fink_grb.grb_utils.grb_prob import p_ser_grb_vect
from fink_grb.init import get_config, init_logging
from fink_broker.science import ang2pix


def compute_healpix_column(spark_df, ra, dec, nside):
    """
    Compute a columns of pixels id and add it to the spark_df dataframe.

    Parameters
    ----------
    spark_df : Spark Distributed dataframe
    ra: Spark dataframe columns
    dec : Spark dataframe columns
    nside : resolution of the healpix map.

    Returns
    -------
    spark_df : Spark Distributed dataframe
        The initial spark_df with a new columns called 'hpix' containing the pixel ids.
    """
    
    spark_df = spark_df.withColumn("hpix", ang2pix(ra, dec, F.lit(nside)))

    return spark_df


@pandas_udf(DoubleType())
def grb_assoc(
    ztf_ra,
    ztf_dec,
    jdstarthist,
    instruments,
    trigger_time,
    grb_ra,
    grb_dec,
    grb_error,
    units,
):
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


def ztf_join_gcn_stream(arguments):
    """
    Join the ztf alerts stream and the gcn stream to find the counterparts of the gcn alerts
    in the ztf stream.

    Parameters
    ----------
    arguments : dictionnary
        arguments parse by docopt from the command line

    Returns
    -------
    """
    config = get_config(arguments)
    logger = init_logging()
    _ = init_sparksession("fink_grb")

    NSIDE = 4

    night = arguments["--night"]
    exit_after = arguments["--exit_after"]

    ztf_datapath_prefix = config["PATH"]["online_ztf_data_prefix"]
    ztf_rawdatapath = ztf_datapath_prefix + "/raw"
    scitmpdatapath = ztf_datapath_prefix + "/science"
    checkpointpath_sci_tmp = ztf_datapath_prefix + "/science_checkpoint"

    # connection to the ztf science stream
    df_ztf_stream = connect_to_raw_database(
        ztf_rawdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        ztf_rawdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        latestfirst=False,
    )

    gcn_datapath_prefix = config["PATH"]["online_gcn_data_prefix"]
    gcn_rawdatapath = gcn_datapath_prefix + "/raw"

    # connection to the gcn stream
    df_grb_stream = connect_to_raw_database(
        gcn_rawdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        gcn_rawdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        latestfirst=True,
    )

    # compute healpix column for each streaming df
    df_ztf_stream = compute_healpix_column(
        df_ztf_stream, df_ztf_stream.candidate.ra, df_ztf_stream.candidate.dec, NSIDE
    )

    df_grb_stream = compute_healpix_column(
        df_grb_stream, df_grb_stream.ra, df_grb_stream.dec, NSIDE
    )

    # join the two streams according to the healpix columns.
    # A pixel id will be assign to each alerts / gcn according to their position in the sky.
    # Each alerts / gcn with the same pixel id are in the same area of the sky.
    # The NSIDE correspond to a resolution of ~15 degree/pixel.
    df_grb = df_ztf_stream.join(
        df_grb_stream, df_ztf_stream["hpix"] == df_grb_stream["hpix"]
    )


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
            "candidate.ra",
            "candidate.dec",
            "candidate.jd",
            "instruments",
            "trigger_id",
            "ra",
            "dec",
            "err",
            "timeUTC",
            "grb_proba",
        ]
    )

    query_grb = (
        df_grb.writeStream.outputMode("append")
        .format("parquet")
        .option("checkpointLocation", checkpointpath_sci_tmp)
        .option("path", scitmpdatapath)
        .partitionBy("year", "month", "day")
        .trigger(processingTime="{} seconds".format(int(config["STREAM"]["tinterval"])))
        .start()
    )

    # Keep the Streaming running until something or someone ends it!
    if exit_after is not None:
        time.sleep(exit_after)
        query_grb.stop()
        logger.info("Exiting the science2grb service normally...")
    else:
        # Wait for the end of queries
        query_grb.awaitAnyTermination()
