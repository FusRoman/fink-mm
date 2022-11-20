from fink_utils.broker.sparkUtils import init_sparksession, connect_to_raw_database


def grb_distribution(grbdatapath, night):


    spark = init_sparksession(
        "science2grb_offline_{}{}{}".format(night[0:4], night[4:6], night[6:8])
    )

    # connection to the grb database
    df_grb_stream = connect_to_raw_database(
        gcn_rawdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        gcn_rawdatapath
        + "/year={}/month={}/day={}".format(night[0:4], night[4:6], night[6:8]),
        latestfirst=True,
    )