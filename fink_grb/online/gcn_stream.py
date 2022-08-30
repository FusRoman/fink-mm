import argparse

import pyarrow as pa
import pyarrow.parquet as pq

import voeventparse as vp

from gcn_kafka import Consumer
from instruments import FERMI, SWIFT, INTEGRAL, ICECUBE, LISTEN_PACKS, INSTR_SUBSCRIBES, detect_instruments

import io
import os
import configparser

import gcn_reader as gr


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

            try:
                voevent = gr.load_voevent(io.StringIO(decode))
            except Exception as e:
                print(decode)
                print(e)
                print()
                print()
                continue

            toplevel_params = vp.get_toplevel_params(voevent)
            gcn_packet_type = toplevel_params["Packet_Type"]["value"]

            gcn_how_description = voevent.How.Description

            if gr.is_observation(voevent) and gr.is_listened_packets_types(voevent, LISTEN_PACKS):

                df = gr.voevent_to_df(voevent)

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