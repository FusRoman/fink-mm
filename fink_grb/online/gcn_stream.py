import pandas as pd
import argparse

import pyarrow as pa
import pyarrow.parquet as pq

from gcn_kafka import Consumer
from instruments import FERMI, SWIFT, INTEGRAL, ICECUBE, LISTEN_PACKS, INSTR_SUBSCRIBES, detect_instruments

import voeventparse as vp
import io
import os
import configparser


def getargs(parser: argparse.ArgumentParser) -> argparse.Namespace:
    """
    Parse command line arguments for fink grb service

    Parameters
    ----------
    parser: argparse.ArgumentParser
        Empty parser

    Returns
    ----------
    args: argparse.Namespace
        Object containing CLI arguments parsedservices

    Examples
    ----------
    >>> import argparse
    >>> parser = argparse.ArgumentParser(description=__doc__)
    >>> args = getargs(parser)
    >>> print(type(args))
    <class 'argparse.Namespace'>
    """
    parser.add_argument(
        '--config', type=str, default="",
        help="""
        Path of the config file.
        Default is ''.
        """)
    args = parser.parse_args(None)
    return args


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


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # read the config file
    config = configparser.ConfigParser(os.environ)
    config.read(args.config)

    # Connect as a consumer.
    # Warning: don't share the client secret with others.
    consumer = Consumer(
        client_id=config["CLIENT"]["id"], client_secret=config["CLIENT"]["secret"]
    )

    # Subscribe to topics and receive alerts
    consumer.subscribe(INSTR_SUBSCRIBES)

    i = 54
    while True:
        for message in consumer.consume():
            value = message.value()
            
            decode = io.BytesIO(value).read().decode("UTF-8")
            voevent = load_voevent(decode)

            toplevel_params = vp.get_toplevel_params(voevent)
            gcn_packet_type = toplevel_params["Packet_Type"]["value"]

            gcn_how_description = voevent.How.Description

            if is_observation(voevent) and is_listened_packets_types(voevent, LISTEN_PACKS):

                df = voevent_to_df(voevent)

                df['year'] = df['timeUTC'].dt.strftime('%Y')
                df['month'] = df['timeUTC'].dt.strftime('%m')
                df['day'] = df['timeUTC'].dt.strftime('%d')

                table = pa.Table.from_pandas(df)

                pq.write_to_dataset(
                    table,
                    root_path=config["PATH"]["gcn_path_storage"],
                    partition_cols=['year', 'month', 'day'],
                    basename_template="{}_{}".format(str(df["trigger_id"].values[0]), "{i}"),
                    existing_data_behavior="overwrite_or_ignore"
                )