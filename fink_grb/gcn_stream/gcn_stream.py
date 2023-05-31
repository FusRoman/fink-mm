import signal
import pyarrow as pa
import pyarrow.parquet as pq
import os

from gcn_kafka import Consumer
import logging

from pyarrow.fs import FileSystem

import fink_grb.gcn_stream.gcn_reader as gr
from fink_grb.init import get_config, init_logging
from fink_grb.utils.fun_utils import return_verbose_level, get_hdfs_connector
from fink_grb.observatory import TOPICS, TOPICS_FORMAT


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


def load_and_parse_gcn(
    gcn: bytes,
    topic: str,
    gcn_rawdatapath: str,
    logger: logging.Logger,
    logs: bool,
    is_test: bool,
    gcn_fs: FileSystem = None,
):
    """
    Load and parse a gcn coming from the gcn kafka stream.

    Parameters
    ----------
    gcn : bytes
        the new gcn coming from the stream
    topic : str
        the emitting topic
    gcn_rawdatapath : string
        the path destination where to store the decoded gcn
    logger : logger object
        logger object for logs.
    logs: boolean
        if true, print logs
    is_test: boolean
        run the function in test mode
    gcn_fs: FileSystem
        the file system used to write the gcn

    Returns
    -------
    None

    Examples
    --------

    >>> f = open('fink_grb/test/test_data/voevent_number=9897.xml').read().encode("UTF-8")
    >>> with tempfile.TemporaryDirectory() as tmp_dir_gcn:
    ...     load_and_parse_gcn(f, "gcn.classic.voevent.FERMI_GBM_FIN_POS", tmp_dir_gcn, logger, False, False)
    ...     base_gcn = pd.read_parquet(tmp_dir_gcn + "/year=2022/month=08/day=30/683571622_0")
    ...     base_gcn = base_gcn.drop(columns="ackTime")
    ...     test_gcn = pd.read_parquet("fink_grb/test/test_data/683571622_0_test")
    ...     assert_frame_equal(base_gcn, test_gcn)

    >>> json_str = open(lvk_initial_path, 'r').read()
    >>> with tempfile.TemporaryDirectory() as tmp_dir_gcn:
    ...     load_and_parse_gcn(json_str, "igwn.gwalert", tmp_dir_gcn, logger, False, False)
    ...     base_gcn = pd.read_parquet(tmp_dir_gcn + "/year=2023/month=05/day=18/S230518h_0")
    ...     base_gcn = base_gcn.drop(columns="ackTime")
    ...     test_gcn = pd.read_parquet("fink_grb/test/test_data/S230518h_0_test")
    ...     assert_frame_equal(base_gcn, test_gcn)
    """

    if topic in TOPICS_FORMAT["xml"]:
        try:
            df = gr.parse_xml_alert(gcn, logger, logs)
        except Exception as e:  # pragma: no cover
            logger.error(
                "Error while reading the xml gcn notice: \n\t {}\n\n\tcause: {}".format(
                    gcn, e
                )
            )
            print()
            return

    elif topic in TOPICS_FORMAT["json"]:
        try:
            df = gr.parse_json_alert(gcn, logger, logs, is_test)
        except Exception as e:
            logger.error("error while reading the json notice\n\t:cause: {}".format(e))
            return

    else:
        logger.error(
            "error while parsing the gcn file:\n\ttopic: {}\n\tgcn: {}".format(
                topic, gcn
            )
        )
        raise Exception("bad gcn file format")

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


def start_gcn_stream(arguments):
    """
    Start to listening the gcn stream. It is an infinite loop that wait messages and write on disk
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

    logs = return_verbose_level(config, logger)

    try:
        consumer_config = {
            "group.id": "fink_mm",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }

        consumer = Consumer(
            config=consumer_config,
            client_id=config["CLIENT"]["id"],
            client_secret=config["CLIENT"]["secret"],
        )

        if arguments["--test"]:
            consumer_config = {"group.id": "", "auto.offset.reset": "earliest"}

            consumer = Consumer(
                config=consumer_config,
                client_id=config["CLIENT"]["id"],
                client_secret=config["CLIENT"]["secret"],
                domain="test.gcn.nasa.gov",
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
    consumer.subscribe(TOPICS)

    signal.signal(signal.SIGINT, signal_handler)

    try:
        gcn_datapath_prefix = config["PATH"]["online_gcn_data_prefix"]
        gcn_rawdatapath = gcn_datapath_prefix
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
            exit(1)

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
                topic = gcn.topic()

                load_and_parse_gcn(
                    value,
                    topic,
                    gcn_rawdatapath,
                    logger,
                    logs,
                    is_test=arguments["--test"],
                    gcn_fs=gcn_fs,
                )
                consumer.commit(gcn)
