import signal
import pyarrow as pa
import pyarrow.parquet as pq

from gcn_kafka import Consumer
from fink_grb.online.instruments import LISTEN_PACKS, INSTR_SUBSCRIBES

import io
import logging

import fink_grb.online.gcn_reader as gr
from fink_grb.init import get_config, init_logging


def signal_handler(signal, frame):
    """
    The signal handler function for the gcn stream.
    Quit the gcn stream by using keyboard command (like Ctrl+C).

    Parameters
    ----------
    signal : integer
        the signal number
    frame :
        the current stack frame
    Returns
    -------
    None
    """
    logging.warn("exit the gcn streaming !")
    exit(0)


def write_and_parse_gcn(gcn, gcn_rawdatapath, logger):
    logger.info("A new voevent is coming")
    value = gcn.value()

    decode = io.BytesIO(value).read()# .decode("UTF-8")

    try:
        voevent = gr.load_voevent(io.BytesIO(value))
    except Exception as e:
        logger.error(
            "Error while reading the following voevent: \n\t {}\n\n\tcause: {}".format(
                value, e
            )
        )
        print()
        return

    if gr.is_observation(voevent) and gr.is_listened_packets_types(
        voevent, LISTEN_PACKS
    ):

        logger.info("the voevent is a new obervation.")

        df = gr.voevent_to_df(voevent)

        df["year"] = df["timeUTC"].dt.strftime("%Y")
        df["month"] = df["timeUTC"].dt.strftime("%m")
        df["day"] = df["timeUTC"].dt.strftime("%d")

        table = pa.Table.from_pandas(df)

        pq.write_to_dataset(
            table,
            root_path=gcn_rawdatapath,
            partition_cols=["year", "month", "day"],
            basename_template="{}_{}".format(
                str(df["trigger_id"].values[0]), "{i}"
            ),
            existing_data_behavior="overwrite_or_ignore",
        )

        logger.info(
            "writing of the new voevent successfull at the location {}".format(
                gcn_rawdatapath
            )
        )
        return
    
    return


def start_gcn_stream(arguments):
    """
    Start to listening the gcn stream. It is an infinite loop that wait messages and the write on disk
    the gnc.

    Parameters
    ----------
    arguments : dictionnary
        arguments parse by docopt from the command line

    Returns
    -------
    None
    """
    config = get_config(arguments)
    logger = init_logging()

    # Connect as a consumer.
    # Warning: don't share the client secret with others.
    consumer = Consumer(
        client_id=config["CLIENT"]["id"], client_secret=config["CLIENT"]["secret"]
    )

    # Subscribe to topics and receive alerts
    consumer.subscribe(INSTR_SUBSCRIBES)

    signal.signal(signal.SIGINT, signal_handler)

    gcn_datapath_prefix = config["PATH"]["online_gcn_data_prefix"]
    gcn_rawdatapath = gcn_datapath_prefix + "/raw"

    while True:
        message = consumer.consume(timeout=2)

        if len(message) != 0:
            for gcn in message:
                write_and_parse_gcn(gcn, gcn_rawdatapath, logger)
