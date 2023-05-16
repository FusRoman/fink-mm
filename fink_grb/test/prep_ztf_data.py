import numpy as np
import voeventparse as vp

from fink_grb.utils.fun_utils import get_observatory


def spatial_time_align(ztf_raw_data, gcn_pdf):
    """Change data in the ztf test alerts to have some fake counterparts of gcn alerts.
    Used by the integration test

    Args:
        ztf_raw_data (DataFrame): ztf test alerts
        gcn_pdf (DataFrame): gnc get from the gcn stream

    Returns:
        DataFrame: the ztf test alerts same as the input but with additionnal alerts which are fake gcn counterparts.
    """
    ztf_raw_data = ztf_raw_data.copy()

    all_obs = gcn_pdf["raw_event"].map(get_observatory).values

    # select half of the gcn alerts
    random_obs = np.random.choice(all_obs, int((len(all_obs) + 1) / 2))

    # get the trigger time and the coordinates for each selected gcn
    jd_gcn = [obs.get_trigger_time()[1] for obs in random_obs]
    coord_gcn = [vp.get_event_position(obs.voevent) for obs in random_obs]

    # select the same number of ztf alerts than the number of selected gcn alerts
    rand_ztf_index = np.random.choice(ztf_raw_data.index, len(random_obs))
    for rows_ztf_cand, rows_ztf_prv, new_jd in zip(
        ztf_raw_data.loc[rand_ztf_index, "candidate"],
        ztf_raw_data.loc[rand_ztf_index, "prv_candidates"],
        jd_gcn,
    ):
        # set their jd and jdstarthist after the trigger time of the gcn
        rows_ztf_cand["jdstarthist"] = new_jd + np.random.uniform(1, 5)
        rows_ztf_prv[0]["jd"] = rows_ztf_cand["jdstarthist"] + np.random.uniform(0.1, 2)

        rows_ztf_cand["ra"] = new_coord.ra
        rows_ztf_cand["dec"] = new_coord.dec

    # for some other alerts, remove the history and set their jdstarthist after the trigger time
    # set their coordinates on the gcn alerts.
    rand_ztf_index = np.random.choice(ztf_raw_data.index, len(random_obs))
    for rows_ztf, new_jd, new_coord in zip(
        ztf_raw_data.loc[rand_ztf_index, "candidate"], jd_gcn, coord_gcn
    ):
        rows_ztf["jdstarthist"] = new_jd + np.random.uniform(1, 5)
        rows_ztf["jd"] = rows_ztf["jdstarthist"] + np.random.uniform(0.1, 2)
        rows_ztf["prv_candidates"] = None

        rows_ztf["ra"] = new_coord.ra
        rows_ztf["dec"] = new_coord.dec

    return ztf_raw_data


if __name__ == "__main__":
    import pandas as pd
    from datetime import datetime
    from pathlib import Path

    path_ztf_raw = (
        "fink_grb/test/test_data/ztf_test/online/raw/year=2019/month=09/day=03/"
    )

    path_gcn_data = "fink_grb/ci_gcn_test/year=2019/month=09/day=03/"

    gcn_pdf = pd.read_parquet(path_gcn_data)
    ztf_pdf = pd.read_parquet(path_ztf_raw)

    new_ztf_raw = spatial_time_align(ztf_pdf, gcn_pdf)

    today = datetime.today()

    new_path_ztf_data = Path(
        "fink_grb/test/test_data/ztf_test/online/raw/year={:04d}/month={:02d}/day={:02d}/".format(
            today.year, today.month, today.day
        )
    )
    new_path_ztf_data.mkdir(parents=True, exist_ok=True)
    new_ztf_raw.to_parquet(new_path_ztf_data.joinpath("alert_alt.parquet"))
