import pandas as pd
import voeventparse as vp

from astropy.time import Time

from fink_grb.online.instruments import detect_platform, detect_instruments


def get_trigger_id(voevent):
    """
    Return the trigger_id from a voevent.

    Parameters
    ----------
    voevent : voevent object
        The voevent object.

    Returns
    -------
    trigger_id : integer
        the trigger_id of the voevent, return -1 if not find.

    Examples
    --------
    >>> f = open('fink_grb/test/test_data/voevent_number=9897.xml', 'rb')
    >>> v = load_voevent(f)
    >>> get_trigger_id(v)
    683571622

    >>> f = open('fink_grb/test/test_data/voevent_number=8727.xml', 'rb')
    >>> v = load_voevent(f)
    >>> get_trigger_id(v)
    13698560401984
    """
    toplevel_params = vp.get_toplevel_params(voevent)

    if "TrigID" in toplevel_params:
        return int(toplevel_params["TrigID"]["value"])

    if "AMON_ID" in toplevel_params:
        return int(toplevel_params["AMON_ID"]["value"])

    return -1  # pragma: no cover


def voevent_to_df(voevent):
    """
    Convert a voevent object into a dataframe.

    Parameters
    ----------
    voevent : voevent object
        The voevent object.

    Returns
    -------
    df : dataframe
        A dataframe object containing some informations from the voevent.
        columns descriptions:
            - instruments : the instruments that send the voevent. (Example: Fermi, SWIFT)
            - ivorn : the ivorn of the voevent. (Example: 'ivo://nasa.gsfc.gcn/AMON#ICECUBE_Cascade_Event2022-08-01T04:08:34.26_26_136889_025590129_0')
            - triggerId : the trigger_id of the voevent. (Example: 683499781)
            - ra : right ascension
            - dec : declination
            - err : error box of the grb event (in degree or arcminute depending of the instruments)
            - units : units of the error box
            - timeUTC : trigger time of the voevent in UTC
            - rawEvent : the original voevent in xml format.

    Examples
    --------
    >>> f = open('fink_grb/test/test_data/voevent_number=9897.xml', 'rb')
    >>> v = load_voevent(f)
    >>> v_pdf = voevent_to_df(v)
    >>> test_pdf = pd.read_parquet("fink_grb/test/test_data/683571622_0_test")
    >>> assert_frame_equal(v_pdf, test_pdf)
    """

    ivorn = voevent.attrib["ivorn"]
    platform = detect_platform(ivorn)
    instrument = detect_instruments(ivorn)

    trigger_id = get_trigger_id(voevent)

    coords = vp.get_event_position(voevent)
    time_utc = vp.get_event_time_as_utc(voevent)

    time_jd = Time(time_utc, format="datetime").jd

    if platform == "Fermi":
        error_unit = "deg"
    elif platform == "SWIFT":  # pragma: no cover
        error_unit = "arcmin"
    elif platform == "INTEGRAL":  # pragma: no cover
        error_unit = "arcmin"
    elif platform == "ICECUBE":  # pragma: no cover
        error_unit = "deg"
    else:  # pragma: no cover
        raise ValueError("bad instruments: {}".format(platform))

    df = pd.DataFrame.from_dict(
        {
            "platform": [platform],
            "instrument": [instrument],
            "ivorn": [ivorn],
            "triggerId": [trigger_id],
            "jd": [time_jd],
            "ra": [coords.ra],
            "dec": [coords.dec],
            "err": [coords.err],
            "units": [error_unit],
            "timeUTC": [time_utc],
            "rawEvent": vp.prettystr(voevent),
        }
    )

    return df


def is_observation(voevent):
    """
    Test if the voevent if of type observation.

    Parameters
    ----------
    voevent : voevent object
        The voevent object.

    Returns
    -------
    is_observation : boolean
        Return True if the voevent is of observation type, otherwise return False

    Examples
    --------
    >>> f = open('fink_grb/test/test_data/voevent_number=9897.xml', 'rb')
    >>> v = load_voevent(f)
    >>> is_observation(v)
    True
    """
    gcn_role = voevent.attrib["role"]
    return gcn_role == "observation"


def is_listened_packets_types(voevent, listen_packs):
    """
    Test if the voevent packet type correspond to those we listen to.

    Parameters
    ----------
    voevent : voevent object
        The voevent object.
    listen_pack : integer list
        The packet numbers type that we want to listen.

    Returns
    -------
    is_listen : boolean
        True if the voevent packet type is contained in listen pack, otherwise return False.

    Examples
    --------
    >>> f = open('fink_grb/test/test_data/voevent_number=9897.xml', 'rb')
    >>> v = load_voevent(f)
    >>> from fink_grb.online.instruments import LISTEN_PACKS
    >>> is_listened_packets_types(v, LISTEN_PACKS)
    True
    """
    toplevel_params = vp.get_toplevel_params(voevent)
    gcn_packet_type = toplevel_params["Packet_Type"]["value"]

    return int(gcn_packet_type) in listen_packs


def load_voevent(file, verbose=False):
    """
    Load a voevent from a file object.
    Raise an exception if the voevent cannot be parse.

    Parameters
    ----------
    file : file object
        the file object containing the voevent.
    verbose : boolean
        print additional information if an exception is raised.

    Returns
    -------
    voevent : voevent object
        The voevent object.
    e : exception object
        the exception if the voevent could not be read.

    Examples
    --------
    >>> f = open('fink_grb/test/test_data/voevent_number=9897.xml', 'rb')
    >>> v = load_voevent(f)
    >>> type(v)
    <class 'lxml.objectify.ObjectifiedElement'>
    >>> v.attrib['ivorn']
    'ivo://nasa.gsfc.gcn/Fermi#GBM_Fin_Pos2022-08-30T17:00:17.49_683571622_0-865'
    """
    try:
        voevent = vp.load(file)
        return voevent
    except Exception as e:  # pragma: no cover
        if verbose:
            print(
                "failed to load the voevent:\n\tlocation={}\n\tcause={}".format(file, e)
            )
        raise e


if __name__ == "__main__":  # pragma: no cover
    import sys
    import doctest
    from pandas.testing import assert_frame_equal  # noqa: F401
    import shutil  # noqa: F401
    import io  # noqa: F401

    if "unittest.util" in __import__("sys").modules:
        # Show full diff in self.assertEqual.
        __import__("sys").modules["unittest.util"]._MAX_LENGTH = 999999999

    sys.exit(doctest.testmod()[0])
