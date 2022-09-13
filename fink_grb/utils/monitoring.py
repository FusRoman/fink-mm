from collections import Counter
import psutil
from terminaltables import DoubleTable, AsciiTable
import datetime
import pytz
from fink_grb.init import get_config, init_logging
import pandas as pd
from fink_grb.online.instruments import ALL_INSTRUMENTS


def gcn_stream_monitoring(arguments):  # pragma: no cover
    """
    Print on the terminal informations about the status of the gcn_stream process
    and the gcn data store in disk.

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
    paris_tz = pytz.timezone("Europe/Paris")

    table_info = []
    for proc in psutil.process_iter(
        [
            "pid",
            "name",
            "cmdline",
            "status",
            "memory_percent",
            "cpu_times",
            "create_time",
        ]
    ):
        pr_i = proc.info

        if "gcn_stream" in pr_i["cmdline"] and "start" in pr_i["cmdline"]:

            table_info = [
                ["proc_name", pr_i["name"]],
                ["pid", pr_i["pid"]],
                ["cmdline", pr_i["cmdline"][-4:]],
                ["status", pr_i["status"]],
                ["memory_percent", "{:.4f} %".format(pr_i["memory_percent"])],
                ["cpu_times (in second)", pr_i["cpu_times"]],
                [
                    "create_time",
                    datetime.datetime.fromtimestamp(
                        pr_i["create_time"], paris_tz
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                ],
            ]

            proc_status = DoubleTable(table_info, "stream gcn status")

            print()
            print(proc_status.table)

    if len(table_info) == 0:
        logger.info("gcn_stream process not found")

    try:
        gcn_datapath_prefix = config["PATH"]["online_gcn_data_prefix"]
        gcn_rawdatapath = gcn_datapath_prefix + "/raw"
    except Exception as e:
        logger.error("Config entry not found \n\t {}".format(e))
        exit(1)

    pdf_gcn = pd.read_parquet(gcn_rawdatapath)
    if len(pdf_gcn) == 0:
        logger.info("no gcn store at the location {}".format(gcn_rawdatapath))
        exit(0)

    print()
    print()

    gcn_table = [["number of gcn", len(pdf_gcn)]]

    tmp_table = []
    for instr in ALL_INSTRUMENTS:
        df_platform = pdf_gcn[pdf_gcn["platform"] == str(instr)]
        tmp_table += [
            [
                "number of gcn for {}".format(str(instr)),
                len(df_platform),
            ]
        ]

        instr_count = Counter(df_platform["instrument_or_event"])
        for k, v in instr_count.items():
            tmp_table += [["", "{} count: {}".format(k, v)]]

    gcn_table += tmp_table

    gcn_table += [
        ["first gcn data (UTC)", pdf_gcn.iloc[0]["timeUTC"]],
        ["last gcn data (UTC)", pdf_gcn.iloc[-1]["timeUTC"]],
    ]

    gcn_data = AsciiTable(gcn_table, "gcn data")

    print(gcn_data.table)
