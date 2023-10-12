import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime
from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy.time import Time
import os
import pathlib
import requests
import io
from fink_science.image_classification.utils import img_normalizer
import altair as alt

plt.rcParams["figure.figsize"] = (20, 10)

plt.rcParams["figure.dpi"] = 150

import seaborn as sns

sns.set_context("poster")

from fink_utils.sso.spins import color_correction_to_V
from fink_filters.filter_mm_module.filter import (
    grb_bronze_events,
    grb_silver_events,
    grb_gold_events,
    gw_bronze_events,
)


class DataJoin:
    def __init__(self, online_pdf) -> None:
        self.online_pdf = online_pdf
        self.online_pdf["triggerTimejd"] = Time(
            self.online_pdf["triggerTimeUTC"].values
        ).jd
        self.online_pdf["ackTimejd"] = Time(self.online_pdf["ackTime"].values).jd

        if "grb_loc_error" in self.online_pdf:
            self.online_pdf["gcn_loc_error"] = self.online_pdf["gcn_loc_error"].fillna(
                self.online_pdf["grb_loc_error"]
            )

        if "grb_ra" in self.online_pdf:
            self.online_pdf["gcn_ra"] = self.online_pdf["gcn_ra"].fillna(
                self.online_pdf["grb_ra"]
            )

        if "grb_dec" in self.online_pdf:
            self.online_pdf["gcn_dec"] = self.online_pdf["gcn_dec"].fillna(
                self.online_pdf["grb_dec"]
            )

        cols = ["gcn_ra", "gcn_dec", "p_assoc", "gcn_loc_error"]
        bronze_args = [
            self.online_pdf["fink_class"],
            self.online_pdf["observatory"],
            self.online_pdf["rb"],
        ]
        silver_args = bronze_args + [self.online_pdf[cols[2]]]
        gold_args = bronze_args + [
            self.online_pdf[cols[3]],
            self.online_pdf[cols[2]],
            self.online_pdf["rate"],
        ]

        if "is_grb_bronze" not in self.online_pdf:
            grb_bronze_mask = grb_bronze_events(*bronze_args)
            self.online_pdf["is_grb_bronze"] = grb_bronze_mask

        if "is_gw_bronze" not in self.online_pdf:
            gw_bronze_mask = gw_bronze_events(*bronze_args)
            self.online_pdf["is_gw_bronze"] = gw_bronze_mask

        if "is_grb_silver" not in self.online_pdf:
            grb_silver_mask = grb_silver_events(*silver_args)
            self.online_pdf["is_grb_silver"] = grb_silver_mask

        if "is_grb_gold" not in self.online_pdf:
            grb_gold_mask = grb_gold_events(*gold_args)
            self.online_pdf["is_grb_gold"] = grb_gold_mask

        self.online_pdf_gb = (
            online_pdf.sort_values("jd")
            .groupby(["objectId", "triggerId", "gcn_status"])
            .agg(
                nb_point=("jd", "count"),
                first_jd=("jd", lambda x: list(x)[0]),
                gcn_time=("triggerTimejd", lambda x: list(x)[0]),
                fid=("fid", lambda x: list(x)[0]),
            )
        )

    def bar_plot_filter(self):
        nb_no_filter = nb_grb_bronze = len(
            self.online_pdf[
                ~self.online_pdf["is_grb_bronze"]
                & ~self.online_pdf["is_grb_silver"]
                & ~self.online_pdf["is_grb_gold"]
            ]
        )

        nb_grb_bronze = len(
            self.online_pdf[
                self.online_pdf["is_grb_bronze"]
                & ~self.online_pdf["is_grb_silver"]
                & ~self.online_pdf["is_grb_gold"]
            ]
        )
        nb_grb_silver = len(
            self.online_pdf[
                self.online_pdf["is_grb_silver"] & ~self.online_pdf["is_grb_gold"]
                & (self.online_pdf["p_assoc"] != -1.0)
            ]
        )

        nb_grb_gold = len(self.online_pdf[self.online_pdf["is_grb_gold"]& (self.online_pdf["p_assoc"] != -1.0)])
        plt.bar("no_filter", nb_no_filter)
        plt.bar("is_grb_bronze", nb_grb_bronze)
        plt.bar("is_grb_silver", nb_grb_silver)
        plt.bar("is_grb_gold", nb_grb_gold)
        plt.yscale("log")
        plt.show()

    def nb_point_distrib(self):
        plt.hist(self.online_pdf_gb["nb_point"], bins=30)
        plt.yscale("log")
        plt.xlabel("ZTF object number of matching point")
        plt.show()

    def delay_distrib(self):
        plt.hist(
            self.online_pdf_gb["first_jd"] - self.online_pdf_gb["gcn_time"], bins=100
        )
        plt.xlabel("delay between the triggerTime and the first ZTF point")
        plt.show()

    def mag_filter_distribution(self):
        plt.bar("r band", self.online_pdf_gb["fid"].value_counts().loc[2])
        plt.bar("g band", self.online_pdf_gb["fid"].value_counts().loc[1])
        plt.show()

    def bar_plot_unique_gcn(self):
        gb_instr = (
            self.online_pdf[["observatory", "instrument", "triggerId"]]
            .drop_duplicates()
            .groupby(["observatory", "instrument"])
            .count()
        )

        t = gb_instr.reset_index()

        _, ax = plt.subplots()
        bars = ax.bar(t["observatory"] + "_" + t["instrument"], gb_instr["triggerId"])
        ax.bar_label(bars)
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.show()

    def plot_nb_match_vs_latitude(self, which_lat):
        """

        Parameters
        ----------
        which_lat : str
            must be either 'gal' or 'ecl'
        """
        gcn_coord = SkyCoord(
            self.online_pdf["gcn_ra"].values,
            self.online_pdf["gcn_dec"].values,
            unit=u.deg,
        )
        self.online_pdf["galactic_longitude"] = gcn_coord.galactic.l.value
        self.online_pdf["galactic_latitude"] = gcn_coord.galactic.b.value

        self.online_pdf["ecliptic_latitude"] = gcn_coord.transform_to(
            "geocentricmeanecliptic"
        ).lat.value
        self.online_pdf["ecliptic_longitude"] = gcn_coord.transform_to(
            "geocentricmeanecliptic"
        ).lon.value

        gb_lat = self.online_pdf.groupby("triggerId").agg(
            nb_match=("objectId", "count"),
            gal_lat=("galactic_latitude", lambda x: list(x)[0]),
            ecl_lat=("ecliptic_latitude", lambda x: list(x)[0]),
        )

        if which_lat == "gal":
            lat_col = "gal_lat"
            label_lat = "galactic"
        elif which_lat == "ecl":
            lat_col = "ecl_lat"
            label_lat = "ecliptic"

        gb_sort_gal = gb_lat.sort_values(lat_col)
        plt.plot(gb_sort_gal[lat_col], gb_sort_gal["nb_match"])
        plt.xlabel(f"{label_lat} latitude")
        plt.ylabel("nb match")
        plt.show()

    def gcn_distribution(self):
        unique_tr = self.online_pdf.drop_duplicates("triggerId")

        plt.figure(figsize=(15, 15), dpi=160)
        plt.subplot(111, projection="aitoff")
        plt.grid(True)
        for obs in unique_tr.observatory.unique():
            tmp_pdf = unique_tr[unique_tr["observatory"] == obs]
            ra, dec = tmp_pdf.gcn_ra, tmp_pdf.gcn_dec
            eq = SkyCoord(ra, dec, unit=u.deg)
            gal = eq.galactic
            plt.scatter(gal.l.wrap_at("180d").radian, gal.b.radian, label=obs, s=50)

        plt.axhline(
            y=np.radians(-30), color="r", linestyle="--", label="ZTF declination limit"
        )
        plt.legend()
        plt.show()

    def bar_timeline(self):
        r = self.online_pdf[["triggerTimeUTC", "objectId"]].sort_values(
            "triggerTimeUTC"
        )
        r["date"] = self.online_pdf["triggerTimeUTC"].dt.to_period("D")
        rr = r.groupby("date").count().reset_index()
        _, ax = plt.subplots()
        ax.bar(
            pd.to_datetime(rr["date"].astype(str)),
            rr["objectId"],
            width=datetime.timedelta(days=1),
        )
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.show()

    def ztf_timeline(self):
        dt_jd = Time(self.online_pdf["jd"], format="jd").to_datetime()
        self.online_pdf["ztf_datetime"] = dt_jd
        self.online_pdf["ztf_date"] = self.online_pdf["ztf_datetime"].dt.to_period("D")
        ztf_date_gb = self.online_pdf[["ztf_date", "objectId"]].groupby("ztf_date").count().reset_index()
        _, ax = plt.subplots()
        ax.bar(
            pd.to_datetime(ztf_date_gb["ztf_date"].astype(str)),
            ztf_date_gb["objectId"],
            width=datetime.timedelta(days=1),
        )
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.show()

    def plot_mag_distrib(self):
        plt.hist(
            self.online_pdf[self.online_pdf["fid"] == 1]["magpsf"],
            bins=100,
            alpha=0.5,
            label="g band",
        )
        plt.hist(
            self.online_pdf[self.online_pdf["fid"] == 2]["magpsf"],
            bins=100,
            alpha=0.5,
            label="r band",
        )
        plt.xlabel("magnitude")
        plt.legend()
        plt.show()

    class GCN:
        def __init__(self, datajoin, triggerId, status) -> None:
            self.triggerId = triggerId
            self.gcn_pdf = datajoin.online_pdf[
                (datajoin.online_pdf["triggerId"] == triggerId)
                & (datajoin.online_pdf["gcn_status"] == status)
            ]

        def info(self):
            one_event = self.gcn_pdf.drop_duplicates(["triggerId", "gcn_status"])
            return one_event[[
                "triggerId", 
                "triggerTimeUTC",
                "observatory",
                "instrument",
                "event",
                "gcn_loc_error",
                "gcn_status",
            ]].to_markdown(index=False, tablefmt="pretty")

        def get_grb_bronze(self):
            return self.gcn_pdf[self.gcn_pdf["is_grb_bronze"]]

        def get_fermi_cat(self):
            grb_fermi = pd.read_table("fermi_gbm_burst_catalog.txt", sep="|")
            grb_fermi.columns = [c.strip() for c in grb_fermi.columns]
            return grb_fermi

        def plot_particular_grb(self, grb_fermi, x_col, y_col, grb_name):
            grb_to_plot = grb_fermi[grb_fermi["name"] == grb_name]
            grb_to_plot = grb_fermi[grb_fermi["name"] == grb_name]

            grb_x = pd.to_numeric(grb_to_plot[x_col], errors='coerce')
            grb_y = pd.to_numeric(grb_to_plot[y_col], errors='coerce')
            x_err = pd.to_numeric(grb_to_plot[f"{x_col}_error"], errors='coerce')
            y_err = pd.to_numeric(grb_to_plot[f"{y_col}_error"], errors='coerce')
            

            plt.errorbar(
                grb_x,
                grb_y,
                xerr=x_err,
                yerr=y_err,
                marker="*", color="red"
            )
            ax = plt.gca()
            ax.annotate(grb_name, xy=(grb_x, grb_y), xytext=(grb_x, grb_y), weight="bold", rotation=45)

        def energy_vs_time_distrib(self, grb_name, grb_170817=False, grb_221009=False):
            grb_fermi = self.get_fermi_cat()

            x_col = "t90"
            y_col = "fluence"

            plt.scatter(
                pd.to_numeric(grb_fermi[x_col], errors='coerce'),
                pd.to_numeric(grb_fermi[y_col], errors='coerce'),
                color='grey', alpha=0.5, s=50
            )

            self.plot_particular_grb(grb_fermi, x_col, y_col, grb_name)

            if grb_170817:
                self.plot_particular_grb(grb_fermi, x_col, y_col, "GRB170817529")
            if grb_221009:
                self.plot_particular_grb(grb_fermi, x_col, y_col, "GRB221009553")

            plt.title("Fermi GBM Burst Catalog\n" + r" $Fluence  Vs  T_{90}$" + " distribution")
            plt.xscale("log")
            plt.yscale("log")
            plt.xlabel(r"$T_{90}[s]$")
            plt.ylabel(r"$Fluence (10-1000 keV)[erg/cm^2]$")
            plt.show()

        def energy_vs_fluence(self, grb_name, grb_170817=False, grb_221009=False):
            grb_fermi = self.get_fermi_cat()

            x_col = "fluence"
            y_col = "flux_1024"

            plt.scatter(
                pd.to_numeric(grb_fermi[x_col], errors='coerce'),
                pd.to_numeric(grb_fermi[y_col], errors='coerce'),
                color='grey', alpha=0.5, s=50
            )

            self.plot_particular_grb(grb_fermi, x_col, y_col, grb_name)

            if grb_170817:
                self.plot_particular_grb(grb_fermi, x_col, y_col, "GRB170817529")
            if grb_221009:
                self.plot_particular_grb(grb_fermi, x_col, y_col, "GRB221009553")

            plt.title("Fermi GBM Burst Catalog\n" + r" $E_p  Vs  Fluence$" + " distribution")
            plt.xscale("log")
            plt.yscale("log")
            plt.xlabel(r"$Fluence (10-1000 keV)[erg/cm^2]$")
            plt.ylabel(r"$E_p (10-1000 keV)[erg]$")
            plt.show()



        def get_grb_silver(self):
            return self.gcn_pdf[self.gcn_pdf["is_grb_silver"] & (self.gcn_pdf["p_assoc"] != -1.0)]

        def get_grb_gold(self):
            return self.gcn_pdf[self.gcn_pdf["is_grb_gold"]]

        def get_gw_bronze(self):
            return self.gcn_pdf[self.gcn_pdf["gw_bronze"]]

        def plot_alert_distribution(self, save_path=None):
            cols = ["gcn_ra", "gcn_dec", "p_assoc", "gcn_loc_error"]

            def plot_grb_scatter(df, ax, marker, size, label):
                if len(df) == 0:
                    return
                deltaRAcosDEC = (df["ztf_ra"] - df[cols[0]]) * np.cos(
                    np.radians(df["ztf_dec"])
                )
                deltaDEC = np.abs((df["ztf_dec"] - df[cols[1]]))

                ax.scatter(
                    deltaRAcosDEC,
                    deltaDEC,
                    alpha=0.5,
                    s=size,
                    marker=marker,
                    label=label,
                )

            def annotate_plot(df, ax):
                deltaRAcosDEC = (df["ztf_ra"] - df[cols[0]]) * np.cos(
                    np.radians(df["ztf_dec"])
                )
                deltaDEC = np.abs((df["ztf_dec"] - df[cols[1]]))

                for dx, dy, objId in zip(deltaRAcosDEC, deltaDEC, df["objectId"]):
                    ax.annotate(objId, xy=(dx, dy), xytext=(dx, dy), weight="bold")

            fig, ax = plt.subplots(subplot_kw={"projection": "polar"}, figsize=(15, 15))
            fig.suptitle(
                "ZTF alert distribution \nfor the GRB triggerId={}".format(
                    self.triggerId
                )
            )

            for _class in self.gcn_pdf["fink_class"].unique():
                tmp_pdf = self.gcn_pdf[self.gcn_pdf["fink_class"] == _class]

                bronze_mask = tmp_pdf["is_grb_bronze"]
                silver_mask = tmp_pdf["is_grb_silver"] & (tmp_pdf["p_assoc"] != -1.0)
                gold_mask = tmp_pdf["is_grb_gold"] & (tmp_pdf["p_assoc"] != -1.0)

                gold_pdf = tmp_pdf[gold_mask]
                silver_pdf = tmp_pdf[~gold_mask & silver_mask]
                bronze_pdf = tmp_pdf[~gold_mask & ~silver_mask & bronze_mask]
                no_filter_pdf = tmp_pdf[~gold_mask & ~silver_mask & ~bronze_mask]

                plot_grb_scatter(
                    no_filter_pdf, ax, "x", 50, "ztf no filter, class={}".format(_class)
                )
                plot_grb_scatter(
                    bronze_pdf, ax, "o", 100, "ztf bronze, class={}".format(_class)
                )

                plot_grb_scatter(
                    silver_pdf, ax, "D", 150, "ztf silver, class={}".format(_class)
                )
                annotate_plot(silver_pdf, ax)

                plot_grb_scatter(
                    gold_pdf, ax, "*", 400, "ztf gold, class={}".format(_class)
                )
                annotate_plot(gold_pdf, ax)

            afterglow_pdf = pd.read_json("afterglow.json")
            afterglow_pdf["triggerId"] = afterglow_pdf["triggerId"].astype(str)
            if self.triggerId in afterglow_pdf["triggerId"].values:
                aft = afterglow_pdf[afterglow_pdf["triggerId"] == self.triggerId]
                aft_ra, aft_dec = aft["afterglow_ra"].values[0], aft["afterglow_dec"].values[0]
                if aft_ra != 0.0 and aft_dec != 0.0:
                    deltaRAcosDEC = (
                        aft_ra - tmp_pdf["gcn_ra"].values[0]
                    ) * np.cos(np.radians(aft_dec))
                    deltaDEC = np.abs((aft_dec - tmp_pdf["gcn_dec"].values[0]))

                    ax.scatter(
                        deltaRAcosDEC,
                        deltaDEC,
                        alpha=0.5,
                        s=400,
                        marker="^",
                        label="afterglow",
                    )

            ax.scatter(0, 0, s=400, alpha=0.8, label=f"triggerId={self.triggerId}")

            label_position = ax.get_rlabel_position()
            ax.text(
                np.radians(label_position + 10),
                ax.get_rmax() / 2.0,
                "Distance from the GRB (degree)",
                rotation=label_position * 1.12,
                ha="center",
                va="center",
                fontsize=25,
                alpha=0.5,
            )
            ax.legend()
            plt.tight_layout()

            if save_path is not None:
                plt.savefig(os.path.join(save_path, f"alert_distrib.png"), transparent=False)
                plt.close()
            else:
                plt.show()

        class ZTF:
            def __init__(self, objectId, gcn) -> None:
                self.gcn = gcn
                self.objectId = objectId
                self.ztf_pdf = gcn.gcn_pdf[gcn.gcn_pdf["objectId"] == self.objectId]

                r = requests.post(
                    "https://fink-portal.org/api/v1/objects",
                    json={
                        "objectId": self.objectId,
                        "withupperlim": "True",
                        "withcutouts": "True",
                    },
                )
                self.fink_data = pd.read_json(io.BytesIO(r.content))

            def plot_lighcurve(self, save_path=None):
                fig = plt.figure(figsize=(15, 6))
                ax1 = fig.add_subplot(111)
                ax2 = ax1.twiny()

                colordic = {1: "C0", 2: "C1"}

                for filt in np.unique(self.fink_data["i:fid"]):
                    maskFilt = self.fink_data["i:fid"] == filt
                    if filt == 1:
                        band = "g band"
                    elif filt == 2:
                        band = "r band"

                    # The column `d:tag` is used to check data type
                    maskValid = self.fink_data["d:tag"] == "valid"
                    ax1.errorbar(
                        self.fink_data[maskValid & maskFilt]["i:jd"].apply(
                            lambda x: x - 2400000.5
                        ),
                        self.fink_data[maskValid & maskFilt]["i:magpsf"],
                        self.fink_data[maskValid & maskFilt]["i:sigmapsf"],
                        ls="",
                        marker="o",
                        color=colordic[filt],
                        label=band,
                        alpha=0.5,
                    )

                    maskUpper = self.fink_data["d:tag"] == "upperlim"
                    ax1.plot(
                        self.fink_data[maskUpper & maskFilt]["i:jd"].apply(
                            lambda x: x - 2400000.5
                        ),
                        self.fink_data[maskUpper & maskFilt]["i:diffmaglim"],
                        ls="",
                        marker="^",
                        color=colordic[filt],
                        markerfacecolor="none",
                        alpha=0.5,
                    )

                    maskBadquality = self.fink_data["d:tag"] == "badquality"
                    ax1.errorbar(
                        self.fink_data[maskBadquality & maskFilt]["i:jd"].apply(
                            lambda x: x - 2400000.5
                        ),
                        self.fink_data[maskBadquality & maskFilt]["i:magpsf"],
                        self.fink_data[maskBadquality & maskFilt]["i:sigmapsf"],
                        ls="",
                        marker="v",
                        color=colordic[filt],
                        alpha=0.5,
                    )

                min_delay = (self.ztf_pdf["jd"] - self.ztf_pdf["triggerTimejd"]).min()
                ax1.axvline(
                    self.ztf_pdf["triggerTimejd"].values[0] - 2400000.5,
                    label=f"triggerTime (delay first point: {min_delay:.4f})",
                    linestyle="--",
                )
                ax1.invert_yaxis()
                ax1.set_xlabel("Modified Julian Date")
                ax1.set_ylabel("Magnitude")
                ax1.legend()

                def tick_function(X):
                    V = X - (self.ztf_pdf["triggerTimejd"].values[0] - 2400000.5)
                    return ["%.3f" % z for z in V]

                ax2.set_xlim(ax1.get_xlim())
                new_ticks = np.linspace(
                    self.ztf_pdf["triggerTimejd"].values[0] - 2400000.5, 
                    ax1.get_xlim()[1],
                    num=len(ax1.get_xticks())
                    )
                ax2.set_xticks(new_ticks)
                ax2.set_xlabel("delay since triggerTime")
                ax2.set_xticklabels(tick_function(new_ticks), rotation=45)

                plt.tight_layout()

                if save_path is not None:
                    plt.savefig(os.path.join(save_path, f"lc.png"), transparent=False)
                    plt.close()
                else:
                    plt.show()

            def plot_images(self, save_path=None):
                columns = [
                    "b:cutoutScience_stampData",
                    "b:cutoutTemplate_stampData",
                    "b:cutoutDifference_stampData",
                ]

                fig, (ax1, ax2, ax3) = plt.subplots(1, 3)
                max_mag = self.fink_data.loc[
                    self.fink_data[self.fink_data["d:tag"] == "valid"][
                        "i:magpsf"
                    ].idxmax()
                ]

                data = np.array(max_mag[columns[0]])
                data = np.where(data == None, 0, data).astype(float)
                ax1.imshow(
                    img_normalizer(data, 0, 255),
                    cmap="gray",
                    vmin=0,
                    vmax=255,
                )
                ax1.set_title("Science")
                fig.suptitle(
                    self.objectId
                    + f" magnitude={max_mag['i:magpsf']:.5f}±{max_mag['i:sigmapsf']:.5f}",
                    y=0.9,
                )

                data = np.array(max_mag[columns[1]])
                data = np.where(data == None, 0, data).astype(float)
                ax2.imshow(
                    img_normalizer(data, 0, 255),
                    cmap="gray",
                    vmin=0,
                    vmax=255,
                )
                ax2.set_title("Template")

                data = np.array(max_mag[columns[2]])
                data = np.where(data == None, 0, data).astype(float)
                ax3.imshow(
                    img_normalizer(data, 0, 255),
                    cmap="gray",
                    vmin=0,
                    vmax=255,
                )
                ax3.set_title("Difference")

                if save_path is not None:
                    plt.savefig(os.path.join(save_path, f"images.png"), transparent=False)
                    plt.close()
                else:
                    plt.show()

            def class_timeline(self, save_path=None):
                r = self.ztf_pdf[["jd", "fink_class", "triggerTimejd"]].sort_values(
                    "jd"
                )
                r["delay"] = r["jd"] - r["triggerTimejd"]
                chart_pdf = pd.DataFrame(
                    {
                        "start": r["delay"].values,
                        "end": list(r["delay"].values[1:])
                        + [r["delay"].values[-1] + 1],
                        "class": r["fink_class"].values,
                    }
                )
                chart = (
                    alt.Chart(chart_pdf)
                    .mark_bar()
                    .encode(
                        alt.X("start").title("delay since trigger time"),
                        alt.X2("end").title("delay since trigger time"),
                        alt.Y("class").title("fink_class"),
                        color=alt.Color("class"),  # .scale(scheme='dark2')
                    )
                    .properties(width="container", height=300)
                )

                if save_path is not None:
                    chart.save(os.path.join(save_path, "class_timeline.png"))
                else:
                    return chart

            def lc_compare(self):
                db_path = 'GRBase_lc_raw.json'

                pdf_grb = pd.read_json(db_path)

                mask = pdf_grb['midtimes'] > 1
                pdf_grb = pdf_grb[mask]

                fig, ax = plt.subplots()
                ax.scatter(pdf_grb["midtimes"], pdf_grb["flux"], color="grey", alpha=0.5, s=5)

                ztf_jd, ztf_mag, ztf_fid = (
                    self.fink_data["i:jd"], 
                    self.fink_data["i:magpsf"], 
                    self.fink_data["i:fid"]
                )


                delay = ztf_jd - self.gcn.gcn_pdf["triggerTimejd"].values[0]

                g_mask = ztf_fid == 1
                with pd.option_context("mode.chained_assignment", None):
                    ztf_mag[g_mask] = (ztf_mag[g_mask] + color_correction_to_V()[1]) - color_correction_to_V()[2]

                ax.scatter(delay * 24 * 3600, ztf_mag, label="r band")

                ax.invert_yaxis()
                ax.set_xscale("log")
                ax.set_xlabel("Time from GRB trigger (second)")
                ax.set_ylabel("magnitude (magpsf)")
                ax.legend()
                plt.show()

        def get_ztf_obj(self, objectId):
            return self.ZTF(objectId, self)

    def get_gcn_join(self, triggerId, status):
        return self.GCN(self, triggerId, status)

    def save_data_analysis(self, triggerId, status, path):
        gcn_class = self.GCN(self, triggerId, status)

        path_to_save = os.path.join(path, triggerId)
        if not os.path.exists(path_to_save):
            pathlib.Path(path_to_save).mkdir(parents=True, exist_ok=True)

        test_pdf = (
            gcn_class.gcn_pdf[
                [
                    "triggerId",
                    "ackTime",
                    "triggerTimeUTC",
                    "observatory",
                    "instrument",
                    "event",
                    "gcn_status",
                    "gcn_loc_error",
                ]
            ]
            .drop_duplicates("triggerId")
            .T
        )
        test_pdf.columns = ["GCN INFO" for _ in test_pdf.columns]
        fig_info = plt.figure(figsize=(3, 0.1))
        ax = plt.subplot(111, frame_on=False)  # no visible frame
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        print(test_pdf.to_markdown())
        try:
            pd.plotting.table(ax, test_pdf)  # where df is your data frame
            plt.tight_layout()
            fig_info.savefig(
                os.path.join(path_to_save, "gcn_info.png"),
                dpi=600,
                transparent=False,
                bbox_inches="tight",
                pad_inches=0.001
            )
        except Exception as e:
            print(f"error when saving table: {e}")
        plt.close()

        gcn_class.plot_alert_distribution(path_to_save)

        silver_or_gold = gcn_class.gcn_pdf[
            (gcn_class.gcn_pdf["is_grb_silver"] | gcn_class.gcn_pdf["is_grb_silver"])
            & (gcn_class.gcn_pdf["p_assoc"] != -1.0)
        ]["objectId"].unique()

        nb_obj = len(silver_or_gold)
        print(f"nb silver or gold alerts: {nb_obj}")
        if nb_obj > 30:
            print("too much ztf object")
            return
        for i, objId in enumerate(silver_or_gold):
            ztf_path = os.path.join(path_to_save, objId)
            if not os.path.exists(ztf_path):
                pathlib.Path(ztf_path).mkdir(parents=True, exist_ok=True)
            print(f"{objId} : {i} / {nb_obj - i} / {nb_obj}")
            obj_class = gcn_class.get_ztf_obj(objId)
            obj_class.plot_images(ztf_path)
            obj_class.plot_lighcurve(ztf_path)
            obj_class.class_timeline(ztf_path)
