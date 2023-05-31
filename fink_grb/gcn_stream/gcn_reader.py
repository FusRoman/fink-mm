import pandas as pd
import voeventparse as vp
import io
from lxml.objectify import ObjectifiedElement
import json
from logging import Logger
from fink_grb.observatory.LVK.LVK import LVK
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
    >>> v = load_voevent_from_path(fermi_gbm_voevent_path)
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
    file: io.BufferedReader, logger: Logger, logs: bool
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
    >>> v = load_voevent_from_file(f)
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
    """
    voevent = load_voevent_from_file(io.BytesIO(gcn), logger)
    observatory = voevent_to_class(voevent)

    if observatory.is_observation() and observatory.is_listened_packets_types():
        if logs:  # pragma: no cover
            logger.info("the voevent is a new obervation.")

        return observatory.voevent_to_df()


def load_json_from_path(file_path: str, logger: Logger, logs: bool = False) -> dict:
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
    """
    if logs:
        logger.info("the alert is a new json gcn")

    record = load_json_from_file(gcn, logger, logs)
    obs_class = json_to_class(record)

    if obs_class.is_observation(is_test) and obs_class.is_listened_packets_types():
        if logs:  # pragma: no cover
            logger.info("the voevent is a new obervation.")
        return obs_class.voevent_to_df()
