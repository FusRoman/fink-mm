import numpy as np

import matplotlib.pyplot as plt
from fink_filters.filter_mm_module.filter import (
    f_grb_bronze_events,
    f_grb_silver_events,
    f_grb_gold_events,
)

def get_gold_and_silver(event_pdf):
    # bronze_mask = bronze_events(event_pdf["fink_class"], event_pdf["rb"])
    silver_mask = f_grb_silver_events(
        event_pdf["fink_class"], event_pdf["rb"], event_pdf["grb_proba"]
    )
    gold_mask = f_grb_gold_events(
        event_pdf["fink_class"], event_pdf["rb"], event_pdf["grb_proba"], event_pdf["rate"]
    )

    gold_pdf = event_pdf[gold_mask]
    silver_pdf = event_pdf[~gold_mask & silver_mask]
    return gold_pdf, silver_pdf

def plot_ztf_join_distribution(event_pdf, eventId):
    def plot_grb_scatter(df, ax, marker, size, label):
        if len(df) == 0:
            return
        deltaRAcosDEC = (df["ztf_ra"] - df["grb_ra"]) * np.cos(
            np.radians(df["ztf_dec"])
        )
        deltaDEC = np.abs((df["ztf_dec"] - df["grb_dec"]))

        ax.scatter(
            deltaRAcosDEC, deltaDEC, alpha=0.5, s=size, marker=marker, label=label
        )

    def annotate_plot(df, ax):
        deltaRAcosDEC = (df["ztf_ra"] - df["grb_ra"]) * np.cos(
            np.radians(df["ztf_dec"])
        )
        deltaDEC = np.abs((df["ztf_dec"] - df["grb_dec"]))

        for dx, dy, objId in zip(deltaRAcosDEC, deltaDEC, df["objectId"]):
            ax.annotate(objId, xy=(dx, dy), xytext=(dx, dy), weight="bold")

    fink_class_event = event_pdf["fink_class"].unique()

    fig, ax = plt.subplots(subplot_kw={"projection": "polar"}, figsize=(15, 15))
    fig.suptitle(
        "ZTF alert distribution \nfor the GRB triggerId={}".format(eventId), fontsize=50
    )

    for _class in fink_class_event:
        tmp_pdf = event_pdf[event_pdf["fink_class"] == _class]

        bronze_mask = f_grb_bronze_events(tmp_pdf["fink_class"], tmp_pdf["rb"])
        silver_mask = f_grb_silver_events(
            tmp_pdf["fink_class"], tmp_pdf["rb"], tmp_pdf["grb_proba"]
        )
        gold_mask = f_grb_gold_events(
            tmp_pdf["fink_class"], tmp_pdf["rb"], tmp_pdf["grb_proba"], tmp_pdf["rate"]
        )

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

        plot_grb_scatter(gold_pdf, ax, "*", 400, "ztf gold, class={}".format(_class))
        annotate_plot(gold_pdf, ax)

    ax.scatter(0, 0, s=400, alpha=0.8, label=f"triggerId={eventId}")

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

    ax.tick_params(axis="x", which="major", labelsize=30)
    ax.tick_params(axis="y", which="major", labelsize=25)
    ax.legend(prop={"size": 35})
    plt.tight_layout()

    plt.show()
