import pandas as pd
import voeventparse as vp

from fink_grb.online.instruments import detect_instruments


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
    """
    toplevel_params = vp.get_toplevel_params(voevent)

    if "TrigID" in toplevel_params:
        return int(toplevel_params["TrigID"]["value"])

    if "AMON_ID" in toplevel_params:
        return int(toplevel_params["AMON_ID"]["value"])

    return -1


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
    """

    ivorn = voevent.attrib["ivorn"]
    instruments = detect_instruments(ivorn)

    trigger_id = get_trigger_id(voevent)

    coords = vp.get_event_position(voevent)
    time_utc = vp.get_event_time_as_utc(voevent)

    if instruments == "Fermi":
        error_unit = "deg"
    elif instruments == "SWIFT":
        error_unit = "arcmin"
    elif instruments == "INTEGRAL":
        error_unit = "arcmin"
    elif instruments == "ICECUBE":
        error_unit = "deg"
    else:
        raise ValueError("bad instruments: {}".format(instruments))

    df = pd.DataFrame.from_dict(
        {
            "instruments": [instruments],
            "ivorn": [ivorn],
            "triggerId": [trigger_id],
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
    """
    try:
        voevent = vp.load(file)
        return voevent
    except Exception as e:
        if verbose:
            print(
                "failed to load the voevent:\n\tlocation={}\n\tcause={}".format(file, e)
            )
        raise e
