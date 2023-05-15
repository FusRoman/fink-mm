import numpy as np
from fink_grb.utils.fun_utils import get_observatory


def spatial_time_align(ztf_raw_data, grb_pdf):
    ztf_raw_data = ztf_raw_data.copy()

    all_obs = grb_pdf["raw_event"].map(get_observatory).values
    random_obs = np.random.choice(all_obs, int((len(all_obs) + 1) / 2))
    jd_gcn = [obs.get_trigger_time()[1] for obs in random_obs]

    rand_ztf_index = np.random.choice(ztf_raw_data.index, len(random_obs))
    for rows_ztf_cand, rows_ztf_prv, new_jd in zip(
        ztf_raw_data.loc[rand_ztf_index, "candidate"],
        ztf_raw_data.loc[rand_ztf_index, "prv_candidates"],
        jd_gcn,
    ):
        rows_ztf_cand["jdstarthist"] = new_jd + np.random.uniform(1, 5)
        rows_ztf_prv[0]["jd"] = new_jd + np.random.uniform(0.1, 1)

    rand_ztf_index = np.random.choice(ztf_raw_data.index, len(random_obs))
    for rows_ztf, new_jd in zip(ztf_raw_data.loc[rand_ztf_index, "candidate"], jd_gcn):
        rows_ztf["jdstarthist"] = new_jd + np.random.uniform(1, 5)
        rows_ztf["prv_candidates"] = None

    return ztf_raw_data


if __name__ == "__main__":

    import glob
    import pandas as pd

    path_ztf_raw = glob.glob(
        "fink_grb/test/test_data/ztf_test/online/raw/year=2019/month=09/day=03/"
    )
    path_gcn_data = "fink_grb/ci_gcn_test/year=2019/month=09/day=03/"

    gcn_pdf = pd.read_parquet(path_gcn_data)
    ztf_pdf = pd.read_parquet(path_ztf_raw)

    new_ztf_raw = spatial_time_align(ztf_pdf, gcn_pdf)
    new_ztf_raw.to_parquet(path_ztf_raw + "alert_alt.parquet")
