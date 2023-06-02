from copy import deepcopy
import pandas as pd
import numpy as np

from fink_grb.utils.fun_utils import get_observatory


past_shift_date = np.random.uniform(1, 20, 200)


def spatial_time_align(
    ztf_raw_data: pd.DataFrame, gcn_pdf: pd.DataFrame
) -> pd.DataFrame:
    """Change data in the ztf test alerts to have some fake counterparts of gcn alerts.
    Used by the integration test

    Parameters
    ----------
    ztf_raw_data: DataFrame
            ztf test alerts
    gcn_pdf: DataFrame
            gnc get from the gcn stream

    Returns
    -------
    DataFrame
        the ztf test alerts same as the input but with additionnal alerts which are fake gcn counterparts.
    """
    ztf_raw_data = ztf_raw_data.copy()
    gcn_pdf = gcn_pdf.sort_values("triggerTimejd")

    first_obs = (
        gcn_pdf[["observatory", "raw_event"]]
        .iloc[:-4]
        .apply(lambda x: get_observatory(x[0], x[1]), axis=1)
        .values
    )
    last_obs = (
        gcn_pdf[["observatory", "raw_event"]]
        .iloc[-4:]
        .apply(lambda x: get_observatory(x[0], x[1]), axis=1)
        .values
    )

    # select half of the gcn alerts
    random_obs = np.random.choice(first_obs, int((len(first_obs) + 1) / 2))
    random_obs = np.concatenate([random_obs, last_obs])

    # get the trigger time and the coordinates for each selected gcn
    jd_gcn = [obs.get_trigger_time()[1] for obs in random_obs]
    coord_gcn = [obs.get_most_probable_position() for obs in random_obs]

    # select the same number of ztf alerts than the number of selected gcn alerts
    rand_ztf_index = np.random.choice(ztf_raw_data.index, len(random_obs))
    for rows_idx, new_jd, new_coord in zip(
        rand_ztf_index,
        jd_gcn,
        coord_gcn,
    ):
        # set their jd and jdstarthist after the trigger time of the gcn
        ztf_raw_data.loc[rows_idx, "candidate"][
            "jdstarthist"
        ] = new_jd + np.random.uniform(0.01, 0.3)

        ztf_raw_data.loc[rows_idx, "prv_candidates"][-1]["jd"] = ztf_raw_data.loc[
            rows_idx, "candidate"
        ]["jdstarthist"] + np.random.uniform(0.001, 0.1)

        ztf_raw_data.loc[rows_idx, "candidate"]["jd"] = ztf_raw_data.loc[
            rows_idx, "prv_candidates"
        ][-1]["jd"] + np.random.uniform(0.01, 0.2)

        ztf_raw_data.loc[rows_idx, "candidate"]["ra"] = new_coord[0]
        ztf_raw_data.loc[rows_idx, "candidate"]["dec"] = new_coord[1]

    # create another fake ztf alerts with ra,dec = (0, 0)
    today_time = Time(today)
    for _ in range(10):
        ztf_row = ztf_raw_data.loc[0]
        ztf_row["candidate"]["jdstarthist"] = today_time.jd + np.random.uniform(
            0.01, 0.3
        )

        ztf_row["prv_candidates"][-1]["jd"] = ztf_row["candidate"][
            "jdstarthist"
        ] + np.random.uniform(0.001, 0.1)

        ztf_row["candidate"]["jd"] = ztf_row["prv_candidates"][-1][
            "jd"
        ] + np.random.uniform(0.01, 0.2)

        ztf_row["candidate"]["ra"] = 0
        ztf_row["candidate"]["dec"] = 1
        ztf_raw_data.loc[len(ztf_raw_data)] = ztf_row

    # create fake ztf alerts for previous night (for the offline mode)
    for i in range(200):
        # deepcopy of the dict to avoid the copy of pointer
        ztf_row = deepcopy(ztf_raw_data.loc[0].to_dict())
        ztf_row["candidate"]["jdstarthist"] = today_time.jd - past_shift_date[i]

        ztf_row["prv_candidates"][-1]["jd"] = ztf_row["candidate"][
            "jdstarthist"
        ] + np.random.uniform(0.001, 0.1)

        ztf_row["candidate"]["jd"] = ztf_row["prv_candidates"][-1][
            "jd"
        ] + np.random.uniform(0.01, 0.2)

        ztf_row["candidate"]["ra"] = 0
        ztf_row["candidate"]["dec"] = 1
        ztf_raw_data.loc[len(ztf_raw_data)] = ztf_row

    return ztf_raw_data


if __name__ == "__main__":
    import pandas as pd
    import glob
    from datetime import datetime
    from pathlib import Path
    from astropy.time import Time

    from fink_grb.observatory import INSTR_FORMAT

    # If no gcn exist today, create some with the current date
    today = datetime.today()
    gcn_data_path = "fink_grb/ci_gcn_test/year={:04d}/month={:02d}/day={:02d}/".format(
        today.year, today.month, today.day
    )
    new_path_gcn_today = Path(gcn_data_path)
    new_path_gcn_today.mkdir(parents=True, exist_ok=True)

    path_gcn = glob.glob("fink_grb/ci_gcn_test/*/*/*/*")
    random_gcn = np.random.choice(path_gcn, int((len(path_gcn) + 1) / 2))

    print()
    print("nb gcn: {}".format(len(path_gcn)))
    print("nb random gcn: {}".format(len(random_gcn)))
    print()

    for gcn_p in random_gcn:
        gcn_pdf = pd.read_parquet(gcn_p)
        print(gcn_pdf)
        print()
        gcn_pdf["format"] = gcn_pdf["observatory"].str.lower().map(INSTR_FORMAT)
        gcn_pdf = gcn_pdf[gcn_pdf["format"] == "xml"]

        if len(gcn_pdf) == 0:
            continue

        print(gcn_pdf)
        print("----")

        today_time = Time(today)

        obs = (
            gcn_pdf[["observatory", "raw_event"]]
            .apply(lambda x: get_observatory(x[0], x[1]), axis=1)
            .values[0]
        )
        obs.voevent.WhereWhen.ObsDataLocation[
            0
        ].ObservationLocation.AstroCoords.Time.TimeInstant.ISOTime = today_time.iso

        obs_pdf = obs.voevent_to_df()
        obs_pdf.to_parquet(
            new_path_gcn_today.joinpath("{}_0".format(obs_pdf["triggerId"].iloc[0]))
        )

    # create other fake gcn today with ra,dec = (0, 1)

    # take a gcn event in xml format
    take = False
    i = 0
    while not take:
        gcn_pdf = pd.read_parquet(path_gcn[i])
        gcn_pdf["format"] = gcn_pdf["observatory"].str.lower().map(INSTR_FORMAT)
        gcn_pdf = gcn_pdf[gcn_pdf["format"] == "xml"]
        if len(gcn_pdf) == 0:
            i += 1
        else:
            take = True

    for i in range(10):
        today_time = Time(today)

        obs = (
            gcn_pdf[["observatory", "raw_event"]]
            .apply(lambda x: get_observatory(x[0], x[1]), axis=1)
            .values[0]
        )
        obs.voevent.WhereWhen.ObsDataLocation[
            0
        ].ObservationLocation.AstroCoords.Time.TimeInstant.ISOTime = today_time.iso

        # ra
        obs.voevent.WhereWhen.ObsDataLocation[
            0
        ].ObservationLocation.AstroCoords.Position2D.Value2.C1 = 0

        # dec
        obs.voevent.WhereWhen.ObsDataLocation[
            0
        ].ObservationLocation.AstroCoords.Position2D.Value2.C2 = 1

        # error radius
        obs.voevent.WhereWhen.ObsDataLocation[
            0
        ].ObservationLocation.AstroCoords.Position2D.Error2Radius = 10

        obs.voevent.WhereWhen.ObsDataLocation[
            0
        ].ObservationLocation.AstroCoords.Position2D.attrib["unit"] = "deg"

        obs_pdf = obs.voevent_to_df()
        obs_pdf.to_parquet(new_path_gcn_today.joinpath("{}_0".format(i)))

    # crate fake gcn in the previous night with ra,dec = (0, 0) (for offline mode)
    for i in range(11, 200):
        today_time = Time(today)

        obs = (
            gcn_pdf[["observatory", "raw_event"]]
            .apply(lambda x: get_observatory(x[0], x[1]), axis=1)
            .values[0]
        )
        obs.voevent.WhereWhen.ObsDataLocation[
            0
        ].ObservationLocation.AstroCoords.Time.TimeInstant.ISOTime = Time(
            today_time.jd - past_shift_date[i], format="jd"
        ).iso

        # ra
        obs.voevent.WhereWhen.ObsDataLocation[
            0
        ].ObservationLocation.AstroCoords.Position2D.Value2.C1 = 0

        # dec
        obs.voevent.WhereWhen.ObsDataLocation[
            0
        ].ObservationLocation.AstroCoords.Position2D.Value2.C2 = 1

        # error radius
        obs.voevent.WhereWhen.ObsDataLocation[
            0
        ].ObservationLocation.AstroCoords.Position2D.Error2Radius = 10

        obs.voevent.WhereWhen.ObsDataLocation[
            0
        ].ObservationLocation.AstroCoords.Position2D.attrib["unit"] = "deg"

        obs_pdf = obs.voevent_to_df()
        obs_pdf.to_parquet(new_path_gcn_today.joinpath("{}_0".format(i)))

    # create fake ztf counterparts for the gcn of the current date
    path_ztf_raw = (
        "fink_grb/test/test_data/ztf_test/online/raw/year=2019/month=09/day=03/"
    )

    path_gcn_data = "fink_grb/ci_gcn_test/"

    gcn_pdf = pd.read_parquet(path_gcn_data)
    ztf_pdf = pd.read_parquet(path_ztf_raw)

    new_ztf_raw = spatial_time_align(ztf_pdf, gcn_pdf)

    new_path_ztf_data = Path(
        "fink_grb/test/test_data/ztf_test/online/raw/year={:04d}/month={:02d}/day={:02d}/".format(
            today.year, today.month, today.day
        )
    )
    new_path_ztf_data.mkdir(parents=True, exist_ok=True)
    new_ztf_raw.to_parquet(new_path_ztf_data.joinpath("alert_alt.parquet"))
