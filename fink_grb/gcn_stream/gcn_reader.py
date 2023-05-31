import pandas as pd
import voeventparse as vp
import io
from lxml.objectify import ObjectifiedElement
import json
from logging import Logger
from fink_grb.observatory import voevent_to_class, json_to_class


def load_voevent_from_path(
    file_path: str, logger: Logger, logs: bool = False
) -> ObjectifiedElement:
    """
    Load a voevent from a file object.
    Raise an exception if the voevent cannot be parse.

    Parameters
    ----------
    file : string
        the path of the xml voevent file.
    logs : boolean
        print additional information if an exception is raised.

    Returns
    -------
    voevent : voevent object
        The voevent object.

    Examples
    --------
    >>> v = load_voevent_from_path(fermi_gbm_voevent_path, logger, False)
    >>> type(v)
    <class 'lxml.objectify.ObjectifiedElement'>
    >>> v.attrib['ivorn']
    'ivo://nasa.gsfc.gcn/Fermi#GBM_Fin_Pos2022-07-29T10:17:31.51_680782656_0-655'
    """
    try:
        with open(file_path, "rb") as f:
            return vp.load(f)
    except Exception as e:  # pragma: no cover
        if logs:
            logger.error(
                "failed to load the voevent:\n\tlocation={}\n\tcause={}".format(
                    file_path, e
                )
            )
        raise e


def load_voevent_from_file(
    file: io.BufferedReader, logger: Logger, logs: bool = False
) -> ObjectifiedElement:
    """
    Load a voevent from a file object.
    Raise an exception if the voevent cannot be parse.

    Parameters
    ----------
    file : string
        the path of the xml voevent file.
    logs : boolean
        print additional information if an exception is raised.

    Returns
    -------
    voevent : voevent object
        The voevent object.

    Examples
    --------
    >>> f = open(fermi_gbm_voevent_path, 'rb')
    >>> v = load_voevent_from_file(f, logger, False)
    >>> type(v)
    <class 'lxml.objectify.ObjectifiedElement'>
    >>> v.attrib['ivorn']
    'ivo://nasa.gsfc.gcn/Fermi#GBM_Fin_Pos2022-07-29T10:17:31.51_680782656_0-655'
    """
    try:
        return vp.load(file)
    except Exception as e:  # pragma: no cover
        if logs:
            logger.error(
                "failed to load the voevent:\n\tlocation={}\n\tcause={}".format(file, e)
            )
        raise e


def parse_xml_alert(gcn: bytes, logger: Logger, logs: bool) -> pd.DataFrame:
    """
    parse the gcn as an xml and return a dataframe

    Parameters
    ----------
    gcn: bytes
        the incoming gcn
    logger: Logger
        logger object for logs.
    logs: bool
        if true, print logs

    Returns
    -------
    voevent_df: pd.DataFrame
        a dataframe containing the voevent data

    Examples
    --------
    >>> f = open('fink_grb/test/test_data/voevent_number=9897.xml').read().encode("UTF-8")
    >>> parse_xml_alert(f, logger, False)
      observatory instrument event  ...  year month  day
    0       Fermi        GBM        ...  2022    08   30
    <BLANKLINE>
    [1 rows x 15 columns]
    """
    voevent = load_voevent_from_file(io.BytesIO(gcn), logger)
    observatory = voevent_to_class(voevent)

    if observatory.is_observation() and observatory.is_listened_packets_types():
        if logs:  # pragma: no cover
            logger.info("the voevent is a new obervation.")

        return observatory.voevent_to_df()


def load_json_from_path(file_path: str, logger: Logger, logs: bool = False) -> dict:
    """
    Load the json from a path.
    Raise an exception if unable to load the json

    Parameters
    ----------
    file_path: str
        the file path
    logger: Logger
        the logger object used to print the logs
    logs: bool
        if true, print the logs

    Returns
    -------
    json_dict: dict
        the json return as a python dict

    Example
    -------
    >>> load_json_from_path(lvk_update_path, logger)["superevent_id"]
    'S230518h'
    """
    try:
        with open(file_path, "r") as f:
            return json.loads(f.read())
    except Exception as e:  # pragma: no cover
        if logs:
            logger.error(
                "failed to load the voevent:\n\tlocation={}\n\tcause={}".format(
                    file_path, e
                )
            )
        raise e


def load_json_from_file(gcn: str, logger: Logger, logs: bool) -> dict:
    """
    Load a json from a string
    Raise an exception if unable to load the json.

    Parameters
    gcn: str
        the gcn as a string
    logger: Logger
        the logger object
    logs: bool
        if true, print the logs

    Returns
    -------
    json_dict: dict
        the json return as a python dict

    Examples
    --------
    >>> json_str = open(lvk_update_path, 'r').read()
    >>> load_json_from_file(json_str, logger, False)["superevent_id"]
    'S230518h'
    """
    try:
        return json.loads(gcn)
    except Exception as e:
        if logs:
            logger.error(
                "failed to load the voevent:\n\tgcn={}\n\tcause={}".format(gcn, e)
            )
        raise e


def parse_json_alert(
    gcn: str, logger: Logger, logs: bool, is_test: bool
) -> pd.DataFrame:
    """
    Parse the gcn as a string describing a json and return it as a pandas dataframe

    Parameters
    ----------
    gcn: str
        the gcn event
    logger: Logger
        the logger object
    logs: boolean
        if true, print logs
    is_test: (bool, optional)
        if true, run this function in test mode
        Parse gw event that are mock events
        Defaults to False.

    Returns
    -------
    gw_pdf: pd.DataFrame
        the gw event as a dataframe

    Example
    -------
    >>> json_str = open(lvk_update_path, 'r').read()
    >>> parse_json_alert(json_str, logger, False, False)
      observatory instrument event  ...  year month  day
    0         LVK      H1_L1    gw  ...  2023    05   18
    <BLANKLINE>
    [1 rows x 15 columns]
    """
    if logs:
        logger.info("the alert is a new json gcn")

    record = load_json_from_file(gcn, logger, logs)
    obs_class = json_to_class(record)

    if obs_class.is_observation(is_test) and obs_class.is_listened_packets_types():
        if logs:  # pragma: no cover
            logger.info("the voevent is a new obervation.")
        return obs_class.voevent_to_df()
