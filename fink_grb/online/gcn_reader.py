import pandas as pd
import voeventparse as vp

from fink_grb.online.instruments import detect_instruments

def get_trigger_id(voevent):
    toplevel_params = vp.get_toplevel_params(voevent)

    if "TrigID" in toplevel_params:
        return int(toplevel_params["TrigID"]["value"])

    if "AMON_ID" in toplevel_params:
        return int(toplevel_params["AMON_ID"]["value"])

    return -1


def voevent_to_df(voevent):


    ivorn = voevent.attrib['ivorn']
    instruments = detect_instruments(ivorn)

    trigger_id = get_trigger_id(voevent)
    
    coords = vp.get_event_position(voevent)
    time_utc = vp.get_event_time_as_utc(voevent)

    if instruments == "Fermi":
        error_unit  = "deg"
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
            'instruments': [instruments],
            'ivorn': [ivorn],
            'trigger_id': [trigger_id],
            'ra': [coords.ra],
            'dec': [coords.dec],
            'err': [coords.err],
            'units': [error_unit],
            'timeUTC': [time_utc],
            'raw_event': vp.prettystr(voevent)
        }
    )

    return df


def is_observation(voevent):
    gcn_role = voevent.attrib["role"]
    return gcn_role == "observation"


def is_listened_packets_types(voevent, listen_packs):
    toplevel_params = vp.get_toplevel_params(voevent)
    gcn_packet_type = toplevel_params["Packet_Type"]["value"]

    if int(gcn_packet_type) in listen_packs:
        return True

    return False


def load_voevent(file, verbose=False):
    try:
            v = vp.load(file)
            return v
    except Exception as e:
        if verbose:
            print("failed to load the voevent:\n\tlocation={}\n\tcause={}".format(file, e))
        raise e