def fermi_subscribe():
    return [
        'gcn.classic.voevent.FERMI_GBM_ALERT',
        'gcn.classic.voevent.FERMI_GBM_FIN_POS',
        'gcn.classic.voevent.FERMI_GBM_FLT_POS',
        'gcn.classic.voevent.FERMI_GBM_GND_POS',
        'gcn.classic.voevent.FERMI_GBM_LC',
        'gcn.classic.voevent.FERMI_GBM_POS_TEST',
        'gcn.classic.voevent.FERMI_GBM_SUBTHRESH',
        'gcn.classic.voevent.FERMI_GBM_TRANS',
        'gcn.classic.voevent.FERMI_LAT_GND',
        'gcn.classic.voevent.FERMI_LAT_MONITOR',
        'gcn.classic.voevent.FERMI_LAT_OFFLINE',
        'gcn.classic.voevent.FERMI_LAT_POS_DIAG',
        'gcn.classic.voevent.FERMI_LAT_POS_INI',
        'gcn.classic.voevent.FERMI_LAT_POS_TEST',
        'gcn.classic.voevent.FERMI_LAT_POS_UPD',
        'gcn.classic.voevent.FERMI_LAT_TRANS',
        'gcn.classic.voevent.FERMI_POINTDIR',
        'gcn.classic.voevent.FERMI_SC_SLEW'
    ]


def swift_subscribe():
     return [
        'gcn.classic.voevent.SWIFT_ACTUAL_POINTDIR',
        'gcn.classic.voevent.SWIFT_BAT_ALARM_LONG',
        'gcn.classic.voevent.SWIFT_BAT_ALARM_SHORT',
        'gcn.classic.voevent.SWIFT_BAT_GRB_ALERT',
        'gcn.classic.voevent.SWIFT_BAT_GRB_LC',
        'gcn.classic.voevent.SWIFT_BAT_GRB_LC_PROC',
        'gcn.classic.voevent.SWIFT_BAT_GRB_POS_ACK',
        'gcn.classic.voevent.SWIFT_BAT_GRB_POS_NACK',
        'gcn.classic.voevent.SWIFT_BAT_GRB_POS_TEST',
        'gcn.classic.voevent.SWIFT_BAT_KNOWN_SRC',
        'gcn.classic.voevent.SWIFT_BAT_MONITOR',
        'gcn.classic.voevent.SWIFT_BAT_QL_POS',
        'gcn.classic.voevent.SWIFT_BAT_SCALEDMAP',
        'gcn.classic.voevent.SWIFT_BAT_SLEW_POS',
        'gcn.classic.voevent.SWIFT_BAT_SUB_THRESHOLD',
        'gcn.classic.voevent.SWIFT_BAT_SUBSUB',
        'gcn.classic.voevent.SWIFT_BAT_TRANS',
        'gcn.classic.voevent.SWIFT_FOM_OBS',
        'gcn.classic.voevent.SWIFT_FOM_PPT_ARG_ERR',
        'gcn.classic.voevent.SWIFT_FOM_SAFE_POINT',
        'gcn.classic.voevent.SWIFT_FOM_SLEW_ABORT',
        'gcn.classic.voevent.SWIFT_POINTDIR',
        'gcn.classic.voevent.SWIFT_SC_SLEW',
        'gcn.classic.voevent.SWIFT_TOO_FOM',
        'gcn.classic.voevent.SWIFT_TOO_SC_SLEW',
        'gcn.classic.voevent.SWIFT_UVOT_DBURST',
        'gcn.classic.voevent.SWIFT_UVOT_DBURST_PROC',
        'gcn.classic.voevent.SWIFT_UVOT_EMERGENCY',
        'gcn.classic.voevent.SWIFT_UVOT_FCHART',
        'gcn.classic.voevent.SWIFT_UVOT_FCHART_PROC',
        'gcn.classic.voevent.SWIFT_UVOT_POS',
        'gcn.classic.voevent.SWIFT_UVOT_POS_NACK',
        'gcn.classic.voevent.SWIFT_XRT_CENTROID',
        'gcn.classic.voevent.SWIFT_XRT_EMERGENCY',
        'gcn.classic.voevent.SWIFT_XRT_IMAGE',
        'gcn.classic.voevent.SWIFT_XRT_IMAGE_PROC',
        'gcn.classic.voevent.SWIFT_XRT_LC',
        'gcn.classic.voevent.SWIFT_XRT_POSITION',
        'gcn.classic.voevent.SWIFT_XRT_SPECTRUM',
        'gcn.classic.voevent.SWIFT_XRT_SPECTRUM_PROC',
        'gcn.classic.voevent.SWIFT_XRT_SPER',
        'gcn.classic.voevent.SWIFT_XRT_SPER_PROC',
        'gcn.classic.voevent.SWIFT_XRT_THRESHPIX',
        'gcn.classic.voevent.SWIFT_XRT_THRESHPIX_PROC'
    ]


def icecube_subscribe():
    return [
        'gcn.classic.voevent.ICECUBE_ASTROTRACK_BRONZE',
        'gcn.classic.voevent.ICECUBE_ASTROTRACK_GOLD',
        'gcn.classic.voevent.ICECUBE_CASCADE'
    ]

def integral_subscribe():
    return [
        'gcn.classic.voevent.INTEGRAL_OFFLINE',
        'gcn.classic.voevent.INTEGRAL_POINTDIR',
        'gcn.classic.voevent.INTEGRAL_REFINED',
        'gcn.classic.voevent.INTEGRAL_SPIACS',
        'gcn.classic.voevent.INTEGRAL_WAKEUP',
        'gcn.classic.voevent.INTEGRAL_WEAK'
    ]