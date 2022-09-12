import psutil
from terminaltables import DoubleTable, AsciiTable
import datetime
import pytz
from fink_grb.init import get_config, init_logging
import pandas as pd
from fink_grb.online.instruments import ALL_INSTRUMENTS


def gcn_stream_monitoring(arguments):

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

        for el in pr_i["cmdline"]:
            if el == "gcn_stream":

                table_info = [
                    ["proc_name", pr_i["name"]],
                    ["pid", pr_i["pid"]],
                    ["cmdline", pr_i["cmdline"][-3:]],
                    ["status", pr_i["status"]],
                    ["memory_percent", "{:.2f} %".format(pr_i["memory_percent"] * 100)],
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
    # print(pdf_gcn[["instruments", "timeUTC"]].iloc[[0, -1]])
    print()
    print()

    gcn_table = [["number of gcn", len(pdf_gcn)]]

    gcn_table += [
        [
            "number of gcn for {}".format(str(instr)),
            len(pdf_gcn[pdf_gcn["instruments"] == str(instr)]),
        ]
        for instr in ALL_INSTRUMENTS
    ]

    gcn_table += [
        ["first gcn data (UTC)", pdf_gcn.iloc[0]["timeUTC"]],
        ["last gcn data (UTC)", pdf_gcn.iloc[-1]["timeUTC"]],
    ]

    gcn_data = AsciiTable(gcn_table, "gcn data")

    print(gcn_data.table)
