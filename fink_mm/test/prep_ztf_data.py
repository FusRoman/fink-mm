import pandas as pd
import numpy as np
import fink_mm.test.utils_integration_test as it
import glob
from astropy.time import Time
from pathlib import Path


def generate_data_online(ztf_pdf, gcn_pdf, today, gcn_today_data_path):
    random = np.random.default_rng()
    # create ztf alerts and gcn for the online mode (time = today)
    new_gcn_trigger_id = np.random.uniform(0, 1e5, 10)
    for new_gcn_id in new_gcn_trigger_id:
        new_gcn = it.align_ztf_and_gcn(ztf_pdf, gcn_pdf, today, random, new_gcn_id)
        new_gcn.to_parquet(gcn_today_data_path + "/{}_0.parquet".format(new_gcn_id))


def generate_data_offline(ztf_pdf, gcn_pdf):
    random = np.random.default_rng()
    # create ztf alerts and gcn for the offline mode (past time)
    new_gcn_trigger_id = np.random.uniform(0, 1e5, 15)
    for id, new_gcn_id in enumerate(new_gcn_trigger_id):
        past_time = today - id
        new_gcn = it.align_ztf_and_gcn(ztf_pdf, gcn_pdf, past_time, random)

        gcn_past_str = (
            "fink_mm/ci_gcn_test/year={:04d}/month={:02d}/day={:02d}/".format(
                past_time.to_datetime().year,
                past_time.to_datetime().month,
                past_time.to_datetime().day,
            )
        )
        gcn_past_path = Path(gcn_past_str)
        gcn_past_path.mkdir(parents=True, exist_ok=True)
        new_gcn.to_parquet(str(gcn_past_path) + "/{}_0.parquet".format(new_gcn_id))


def dictify(x, type):
    res = {}
    for el in x:
        res[str(el[0])] = type(el[1])
    return res


if __name__ == "__main__":
    # If no gcn exist today, create some with the current date
    today = Time.now()
    gcn_today_data_path = (
        "fink_mm/ci_gcn_test/year={:04d}/month={:02d}/day={:02d}/".format(
            today.to_datetime().year, today.to_datetime().month, today.to_datetime().day
        )
    )
    new_path_gcn_today = Path(gcn_today_data_path)
    new_path_gcn_today.mkdir(parents=True, exist_ok=True)

    path_gcn = glob.glob("fink_mm/ci_gcn_test/*/*/*/*")
    for p in path_gcn:
        gcn_pdf = pd.read_parquet(p)
        gcn_pdf = it.get_xml_notices(gcn_pdf).reset_index(drop=True)
        if len(gcn_pdf) == 0:
            continue
        else:
            break

    if len(gcn_pdf) == 0:
        raise Exception("no xml alerts, weird !!!")

    # create fake ztf counterparts for the gcn of the current date
    path_ztf_raw = (
        "fink_mm/test/test_data/ztf_test/archive/science/year=2019/month=09/day=03/"
    )
    ztf_pdf = pd.read_parquet(path_ztf_raw)

    generate_data_online(ztf_pdf, gcn_pdf, today, gcn_today_data_path)
    generate_data_offline(ztf_pdf, gcn_pdf)

    archive_path_ztf_data = Path(
        "fink_mm/test/test_data/ztf_test/archive/science/year={:04d}/month={:02d}/day={:02d}/".format(
            today.to_datetime().year, today.to_datetime().month, today.to_datetime().day
        )
    )
    online_path_ztf_data = Path(
        "fink_mm/test/test_data/ztf_test/online/science/year={:04d}/month={:02d}/day={:02d}/".format(
            today.to_datetime().year, today.to_datetime().month, today.to_datetime().day
        )
    )
    archive_path_ztf_data.mkdir(parents=True, exist_ok=True)
    online_path_ztf_data.mkdir(parents=True, exist_ok=True)

    ztf_pdf["t2"] = ztf_pdf["t2"].apply(dictify, args=(float,))
    ztf_pdf["mangrove"] = ztf_pdf["mangrove"].apply(dictify, args=(str,))

    ztf_pdf.to_parquet(archive_path_ztf_data.joinpath("alert_alt.parquet"))
    ztf_pdf.to_parquet(online_path_ztf_data.joinpath("alert_alt.parquet"))
