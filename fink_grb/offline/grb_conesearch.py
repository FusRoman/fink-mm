import pandas as pd
import requests
from astropy.time import Time
import numpy as np
import astropy.units as u
import time as t
import io

from fink_grb.utils.grb_prob import p_ser_grb_vect


def grb_crossmatch(ra, dec, loc_error, error_units, trigger_time, instruments):

    print("error units ", error_units)
    print("loc_error before ", loc_error)
    if error_units == u.degree:
        loc_error = loc_error * 3600
        if loc_error > 5 * 3600:
            loc_error = 5 * 3600
    elif error_units == u.arcminute:
        loc_error = loc_error * 60
    else:
        raise ValueError("incorrect unit: {}".format(error_units))

    print("loc_error after ", loc_error)
    if loc_error == 0:
        loc_error = 1

    t_before = t.time()
    r = requests.post(
        "https://fink-portal.org/api/v1/explorer",
        json={
            "ra": str(ra),
            "dec": str(dec),
            "radius": str(loc_error),
            "startdate_conesearch": str(pd.to_datetime(trigger_time).tz_convert(None)),
            "window_days_conesearch": 2,
        },
    )

    # Format output in a DataFrame
    pdf = pd.read_json(io.BytesIO(r.content))
    print("query time: {}".format(t.time() - t_before))
    print("nb ztf alerts from query: {}".format(len(pdf)))
    if len(pdf) > 0:

        t_before = t.time()

        if instruments == "Fermi":
            grb_det_rate = 250
        elif instruments == "SWIFT":
            grb_det_rate = 100
        elif instruments == "INTEGRAL":
            grb_det_rate = 60
        elif instruments == "ICECUBE":
            grb_det_rate = 8
        else:
            raise ValueError("bad instruments: {}".format(instruments))

        trigger_time = Time(
            pd.to_datetime(trigger_time, utc=True), format="datetime"
        ).jd
        delay_year = (pdf["i:jdstarthist"] - trigger_time) / 365.25

        time_condition = delay_year > 0

        proba = np.ones_like(delay_year) * -1.0

        proba[time_condition] = p_ser_grb_vect(
            loc_error / 3600, delay_year[time_condition], grb_det_rate
        )[0]

        pdf["pser_grb"] = proba

        print("proba time: {}".format(t.time() - t_before))
        return pdf


# if __name__ == "__main__":

#     import datetime

#     pdf_gcn = pd.read_parquet("gcn_test/raw/").astype({"day": int, "month": int, "year": int})


#     d = datetime.datetime.now() - datetime.timedelta(days=4)

#     last_gcn = pdf_gcn[(pdf_gcn["day"] >= int(d.day)) & (pdf_gcn["month"] >= int(d.month)) & (pdf_gcn["year"] >= int(d.year))]

#     t_before = t.time()
#     res_crossmatch = last_gcn.apply(lambda x: grb_crossmatch(
#         x["ra"], 
#         x["dec"], 
#         x["err"],
#         x["units"],
#         x["timeUTC"],
#         x["instruments"]
#         ),
#         axis=1)
#     print(t.time() - t_before)
#     print()
#     for res in res_crossmatch:
#         print(res)
