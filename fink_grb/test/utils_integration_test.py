import pandas as pd
import numpy as np
from copy import deepcopy
from lxml.objectify import ObjectifiedElement


from astropy.time import Time

from fink_grb.utils.fun_utils import get_observatory
from fink_grb.observatory import INSTR_FORMAT


def get_copy_of_row(pdf: pd.DataFrame, index_row: int) -> dict:
    row = pdf.loc[index_row].to_dict()
    return deepcopy(row)


def get_candidate_field(row: dict, field: str):
    return row["candidate"][field]


def get_previous_candidate_field(row: dict, past_index: int, field: str):
    return row["prv_candidates"][past_index][field]


def get_xml_notices(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["format"] = pdf["observatory"].str.lower().map(INSTR_FORMAT)
    pdf = pdf[pdf["format"] == "xml"]
    return pdf


def set_candidate_field(row: dict, field: str, value):
    row["candidate"][field] = value


def set_previous_candidate_field(row: dict, past_index: int, field: str, value):
    row["prv_candidates"][past_index][field] = value


def align_ztf(
    ztf_pdf: pd.DataFrame,
    time: float,
    ra: float,
    dec: float,
    random: np.random.Generator,
):
    """
    Add a new alert to the ztf alert dataframe where the emission time is time
    and the sky localization is ra and dec given in inputs.

    Parameters
    ----------
    ztf_pdf: pd.DataFrame
        dataframe containing ztf alert
    time: float
        emission time of the new alert
    ra: float
        ra localization of the new alert
    dec: float
        dec localization of the new alert
    random: np.random.Generator
        a numpy random generator to create deterministic results

    Examples
    --------
    >>> path_ztf_raw = ("fink_grb/test/test_data/ztf_test/online/raw/year=2019/month=09/day=03/")
    >>> ztf_pdf = pd.read_parquet(path_ztf_raw)

    >>> random = np.random.default_rng(0)
    >>> align_ztf(ztf_pdf, 5, 0, 10, random)

    >>> ztf_pdf.loc[0]["candidate"]["ra"]
    289.4610443

    >>> ztf_pdf.loc[0]["candidate"]["dec"]
    -11.0504023

    >>> ztf_pdf.loc[len(ztf_pdf)-1]["candidate"]["ra"]
    0

    >>> ztf_pdf.loc[len(ztf_pdf)-1]["candidate"]["dec"]
    10

    >>> ztf_pdf.loc[0]["candidate"]["jdstarthist"]
    2458729.6881481
    >>> ztf_pdf.loc[0]["candidate"]["jd"]
    2458729.6881481
    >>> ztf_pdf.loc[0]["prv_candidates"][-1]["jd"]
    2458725.7316204

    >>> ztf_pdf.loc[len(ztf_pdf)-1]["candidate"]["jdstarthist"]
    5.020833333333333
    >>> ztf_pdf.loc[len(ztf_pdf)-1]["candidate"]["jd"]
    5.046891752594755
    >>> ztf_pdf.loc[len(ztf_pdf)-1]["prv_candidates"][-1]["jd"]
    5.0255087883235365
    """
    new_ztf_row = get_copy_of_row(ztf_pdf, 0)

    # 1 hour after the gcn trigger time
    set_candidate_field(new_ztf_row, "jdstarthist", time + 30 / 24 / 60)

    # between 1 minutes and 10 minutes after the jdstarthist
    new_jdstarthist = get_candidate_field(new_ztf_row, "jdstarthist")
    set_previous_candidate_field(
        new_ztf_row,
        -1,
        "jd",
        new_jdstarthist + random.uniform(1 / 24 / 60, 10 / 24 / 60),
    )

    # between 20 minutes and 1 hour after the jd of the previous alert
    new_prv_jd = get_previous_candidate_field(new_ztf_row, -1, "jd")
    set_candidate_field(
        new_ztf_row, "jd", new_prv_jd + random.uniform(20 / 24 / 60, 1 / 24)
    )

    set_candidate_field(new_ztf_row, "ra", ra)
    set_candidate_field(new_ztf_row, "dec", dec)

    # ztf filter
    set_candidate_field(new_ztf_row, "ssdistnr", 30.0)
    set_candidate_field(new_ztf_row, "distpsnr1", 30.0)
    set_candidate_field(new_ztf_row, "neargaia", 30.0)
    set_candidate_field(new_ztf_row, "sgscore1", 0.0)

    ztf_pdf.loc[len(ztf_pdf)] = new_ztf_row


def set_gcn_trigger_time(voevent: ObjectifiedElement, time: str):
    voevent.voevent.WhereWhen.ObsDataLocation[
        0
    ].ObservationLocation.AstroCoords.Time.TimeInstant.ISOTime = time


def set_gcn_coord(voevent: ObjectifiedElement, ra: float, dec: float):
    # ra
    voevent.voevent.WhereWhen.ObsDataLocation[
        0
    ].ObservationLocation.AstroCoords.Position2D.Value2.C1 = ra

    # dec
    voevent.voevent.WhereWhen.ObsDataLocation[
        0
    ].ObservationLocation.AstroCoords.Position2D.Value2.C2 = dec


def set_gcn_error(voevent: ObjectifiedElement, error: float):
    """
    set the error of the gcn

    Args:
    voevent: ObjectifiedElement
    error: float
        error in degree
    """

    # error radius
    voevent.voevent.WhereWhen.ObsDataLocation[
        0
    ].ObservationLocation.AstroCoords.Position2D.Error2Radius = error

    voevent.voevent.WhereWhen.ObsDataLocation[
        0
    ].ObservationLocation.AstroCoords.Position2D.attrib["unit"] = "deg"


def align_xml_gcn(
    gcn_xml_pdf: pd.DataFrame,
    time: str,
    ra: float,
    dec: float,
    error: float,
    gcn_id: int = 0,
) -> pd.DataFrame:
    """
    Return a new gcn alert where the emission time is time
    and the sky localization is ra and dec given in inputs.
    The gcn must be a xml.

    Parameters
    ----------
    gcn_xml_pdf: pd.DataFrame
        input xml gcn
    time: str
        the new trigger time
    ra: float
        the new right ascension of the alert
    dec: float
        the new declination of the alert
    error: float
        error in degree
    gcn_id: int
        the trigger id of the new gcn

    Returns
    -------
    Dataframe:
        the new gcn alert

    Examples
    --------
    >>> path_gcn = "fink_grb/test/test_data/683571622_0_test"
    >>> gcn = pd.read_parquet(path_gcn)

    >>> trigger_time = '2023-06-06 07:33:30.511'
    >>> new_gcn = align_xml_gcn(gcn, trigger_time, 10, 24, 5)

    >>> new_gcn[["ra", "dec"]]
         ra   dec
    0  10.0  24.0

    >>> new_gcn[["triggerTimeUTC", "triggerTimejd"]]
                        triggerTimeUTC  triggerTimejd
    0 2023-06-06 07:33:30.511000+00:00   2.460102e+06

    >>> new_gcn["err_arcmin"]
    0    300.0
    Name: err_arcmin, dtype: float64
    """
    new_gcn = pd.Series(get_copy_of_row(gcn_xml_pdf, 0))
    obs = get_observatory(new_gcn["observatory"], new_gcn["raw_event"])
    set_gcn_trigger_time(obs, time)
    set_gcn_coord(obs, ra, dec)
    set_gcn_error(obs, error)
    new_gcn_pdf = obs.voevent_to_df()
    new_gcn_pdf["triggerId"] = str(gcn_id)
    return new_gcn_pdf


def align_ztf_and_gcn(
    ztf_pdf: pd.DataFrame,
    gcn_pdf: pd.DataFrame,
    time: Time,
    random: np.random.Generator,
    gcn_id: int = 0,
) -> pd.DataFrame:
    """
    Create new gcn alerts and ztf alerts. The ztf_alerts start to vary 1 hour after the gcn.
    The ztf_alert is located right on gcn alert.

    Parameters
    ----------
    ztf_pdf: pd.DataFrame
        ztf alerts
    gcn_pdf: pd.DataFrame
        gcn alert
    gcn_id: int, optional, Defaults to 0.
        the trigger id of the new gcn
    random: np.random.Generator, optional, Defaults to np.random.default_rng().
        random generator for determinism

    Returns
    -------
    pd.DataFrame:
        the new gcn alert
        The ztf alert are added in place to the input dataframe

    Examples
    --------
    >>> today = Time.now()
    >>> path_gcn = "fink_grb/test/test_data/683571622_0_test"
    >>> gcn = pd.read_parquet(path_gcn)

    >>> path_ztf_raw = ("fink_grb/test/test_data/ztf_test/online/raw/year=2019/month=09/day=03/")
    >>> ztf_pdf = pd.read_parquet(path_ztf_raw)

    >>> random = np.random.default_rng(0)
    >>> new_gcn = align_ztf_and_gcn(ztf_pdf, gcn, today, random)

    >>> ztf_pdf.loc[len(ztf_pdf)-1]["candidate"]["ra"] == new_gcn["ra"]
    0    True
    Name: ra, dtype: bool

    >>> ztf_pdf.loc[len(ztf_pdf)-1]["candidate"]["dec"] == new_gcn["dec"]
    0    True
    Name: dec, dtype: bool

    >>> ztf_pdf.loc[len(ztf_pdf)-1]["candidate"]["jdstarthist"] > new_gcn["triggerTimejd"]
    0    True
    Name: triggerTimejd, dtype: bool

    >>> obs = get_observatory(new_gcn["observatory"].values[0], new_gcn["raw_event"].values[0])
    >>> ztf_ra = ztf_pdf.loc[len(ztf_pdf)-1]["candidate"]["ra"]
    >>> ztf_dec = ztf_pdf.loc[len(ztf_pdf)-1]["candidate"]["dec"]
    >>> ztf_jdstarthist = ztf_pdf.loc[len(ztf_pdf)-1]["candidate"]["jdstarthist"]

    >>> grb_proba = obs.association_proba(ztf_ra, ztf_dec, ztf_jdstarthist)
    >>> (1 - grb_proba) > special.erf(5 / sqrt(2))
    True
    """
    ra = random.uniform(0, 360)
    dec = random.uniform(-90, 90)
    error = random.uniform(1, 10)

    new_gcn = align_xml_gcn(gcn_pdf, time.iso, ra, dec, error, gcn_id)
    align_ztf(ztf_pdf, time.jd, ra, dec, random)

    return new_gcn
