import signal
import pyarrow as pa
import pyarrow.parquet as pq
import os

from gcn_kafka import Consumer
from fink_grb.observatory.observatory import LISTEN_PACKS, INSTR_SUBSCRIBES

import io
import logging

import fink_grb.gcn_stream.gcn_reader as gr
from fink_grb.init import get_config, init_logging
from fink_grb.utils.fun_utils import return_verbose_level, get_hdfs_connector


def signal_handler(signal, frame):  # pragma: no cover
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


def load_and_parse_gcn(gcn, gcn_rawdatapath, logger, logs=False, gcn_fs=None):
    """
    Load and parse a gcn coming from the gcn kafka stream.

    Parameters
    ----------
    gcn : bytes
        the new gcn coming from the stream
    gcn_rawdatapath : string
        the path destination where to store the decoded gcn
    logger : logger object
        logger object for logs.

    Returns
    -------
    None

    Examples
    --------

    >>> f = open('fink_grb/test/test_data/voevent_number=9897.xml').read().encode("UTF-8")
    >>> logger = init_logging()
    >>> gcn_test_path = 'fink_grb/test/test_output'
    >>> load_and_parse_gcn(f, gcn_test_path, logger)
    >>> base_gcn = pd.read_parquet(gcn_test_path + "/year=2022/month=08/day=30/683571622_0")
    >>> base_gcn = base_gcn.drop(columns="ackTime")
    >>> test_gcn = pd.read_parquet("fink_grb/test/test_data/683571622_0_test")
    >>> assert_frame_equal(base_gcn, test_gcn)
    >>> shutil.rmtree(gcn_test_path + "/year=2022")
    """
    try:
        voevent = gr.load_voevent(io.BytesIO(gcn))
    except Exception as e:  # pragma: no cover
        logger.error(
            "Error while reading the following voevent: \n\t {}\n\n\tcause: {}".format(
                gcn, e
            )
        )
        print()
        return

    if gr.is_observation(voevent) and gr.is_listened_packets_types(
        voevent, LISTEN_PACKS
    ):

        if logs:  # pragma: no cover
            logger.info("the voevent is a new obervation.")

        df = gr.voevent_to_df(voevent)

        df["year"] = df["triggerTimeUTC"].dt.strftime("%Y")
        df["month"] = df["triggerTimeUTC"].dt.strftime("%m")
        df["day"] = df["triggerTimeUTC"].dt.strftime("%d")

        table = pa.Table.from_pandas(df)

        pq.write_to_dataset(
            table,
            root_path=gcn_rawdatapath,
            partition_cols=["year", "month", "day"],
            basename_template="{}_{}".format(str(df["triggerId"].values[0]), "{i}"),
            existing_data_behavior="overwrite_or_ignore",
            filesystem=gcn_fs,
        )

        if logs:  # pragma: no cover
            logger.info(
                "writing of the new voevent successfull at the location {}".format(
                    gcn_rawdatapath
                )
            )
        return

    return  # pragma: no cover


def start_gcn_stream(arguments):
    """
    Start to listening the gcn stream. It is an infinite loop that wait messages and write on disk
    the gnc.

    Parameters
    ----------
    arguments : dictionnary
        arguments parse by docopt from the command line
    logs : boolean
        activate the logs messages

    Returns
    -------
    None
    """
    config = get_config(arguments)
    logger = init_logging()

    logs = return_verbose_level(config, logger)

    try:
        # Connect as a consumer.
        # Warning: don't share the client secret with others.
        consumer = Consumer(
            client_id=config["CLIENT"]["id"], client_secret=config["CLIENT"]["secret"]
        )
    except Exception as e:
        logger.error("Config entry not found \n\t {}".format(e))
        exit(1)

    try:
        fs_host = config["HDFS"]["host"]
        fs_port = int(config["HDFS"]["port"])
        fs_user = config["HDFS"]["user"]
        gcn_fs = get_hdfs_connector(fs_host, fs_port, fs_user)

    except Exception as e:
        if logs:
            logger.info("config entry not found for hdfs filesystem: \n\t{}".format(e))
        gcn_fs = None

    # Subscribe to topics and receive alerts
    consumer.subscribe(INSTR_SUBSCRIBES)

    signal.signal(signal.SIGINT, signal_handler)

    try:
        gcn_datapath_prefix = config["PATH"]["online_gcn_data_prefix"]
        gcn_rawdatapath = gcn_datapath_prefix + "/raw"
    except Exception as e:
        logger.error("Config entry not found \n\t {}".format(e))
        exit(1)

    if gcn_fs is None:
        if not os.path.exists(gcn_rawdatapath):
            logger.error(
                "Path of the gcn stream output not found in your local file system : {}".format(
                    gcn_rawdatapath
                )
            )

    if logs:
        logger.info(
            "GCN stream initialisation successfull.\nThe deamon is running and wait for gcn arrivals."
        )

    while True:
        message = consumer.consume(timeout=2)

        if len(message) != 0:
            for gcn in message:

                if logs:
                    logger.info("A new voevent is coming")
                value = gcn.value()

                load_and_parse_gcn(value, gcn_rawdatapath, logger, logs, gcn_fs=gcn_fs)


if __name__ == "__main__":  # pragma: no cover
    import sys
    import doctest
    from pandas.testing import assert_frame_equal  # noqa: F401
    import pandas as pd  # noqa: F401
    import shutil  # noqa: F401

    if "unittest.util" in __import__("sys").modules:
        # Show full diff in self.assertEqual.
        __import__("sys").modules["unittest.util"]._MAX_LENGTH = 999999999

    sys.exit(doctest.testmod()[0])
