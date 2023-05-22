import pandas as pd
import voeventparse as vp
import io
from lxml.objectify import ObjectifiedElement
import json
from logging import Logger
from astropy.table import Table
import astropy_healpix as ah
from base64 import b64decode
from fink_grb.observatory.LVK import LVK


def load_voevent_from_path(
    file_path: str, logger: Logger, verbose: bool = False
) -> ObjectifiedElement:
    """
    Load a voevent from a file object.
    Raise an exception if the voevent cannot be parse.

    Parameters
    ----------
    file : string
        the path of the xml voevent file.
    verbose : boolean
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
        if verbose:
            logger.error(
                "failed to load the voevent:\n\tlocation={}\n\tcause={}".format(
                    file_path, e
                )
            )
        raise e


def load_voevent_from_file(
    file: io.BufferedReader, logger: Logger, verbose: bool = False
) -> ObjectifiedElement:
    """
    Load a voevent from a file object.
    Raise an exception if the voevent cannot be parse.

    Parameters
    ----------
    file : string
        the path of the xml voevent file.
    verbose : boolean
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
        if verbose:
            logger.error(
                "failed to load the voevent:\n\tlocation={}\n\tcause={}".format(file, e)
            )
        raise e


def parse_gw_alert(
    txt_file: str, logger: Logger, is_test: bool = False
) -> pd.DataFrame:
    logger.info("the alert is probably a new gw")
    try:
        record = json.loads(record)
    except Exception as e:
        logger.error(
            "failed to load the gw alert:\n\talert={}\n\tcause={}".format(txt_file, e)
        )

    # Only respond to mock events. Real events have GraceDB IDs like
    # S1234567, mock events have GraceDB IDs like M1234567.
    # NOTE NOTE NOTE replace the conditional below with this commented out
    # conditional to only parse real events.
    # if record['superevent_id'][0] != 'S':
    #    return
    event_kind = "S" if not is_test else "M"
    if record["superevent_id"][0] != event_kind:
        return

    lvk_class = LVK(record)

    print(lvk_class)

    if record["alert_type"] == "RETRACTION":
        print(record["superevent_id"], "was retracted")
        return

    # Respond only to 'CBC' events. Change 'CBC' to 'Burst' to respond to
    # only unmodeled burst events.
    if record["event"]["group"] != "CBC":
        return

    # Parse sky map
    skymap_str = record.get("event", {}).pop("skymap")
    if skymap_str:
        # Decode, parse skymap, and print most probable sky location
        skymap_bytes = b64decode(skymap_str)
        skymap = Table.read(io.BytesIO(skymap_bytes))

        # level, ipix = ah.uniq_to_level_ipix(
        #     skymap[np.argmax(skymap["PROBDENSITY"])]["UNIQ"]
        # )

        level, ipix = ah.uniq_to_level_ipix(skymap["UNIQ"])
        print(level)
        print(ipix)

        # print(skymap)
        ra, dec = ah.healpix_to_lonlat(ipix, ah.level_to_nside(level), order="nested")
        print(ra)
        print(dec)
        # print(f"Most probable sky location (RA, Dec) = ({ra.deg}, {dec.deg})")

        # Print some information from FITS header
        # print(f'Distance = {skymap.meta["DISTMEAN"]} +/- {skymap.meta["DISTSTD"]}')

    # Print remaining fields
    # print("Record:")
    # pprint(record)
