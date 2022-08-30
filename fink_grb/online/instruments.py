class Instrument:
    def __init__(self, instruments_name, packet_type):
        self.instruments = instruments_name
        self.packet_type = packet_type

    def __str__(self) -> str:
        return self.instruments

class Fermi(Instrument):
    def __init__(self):

        fermi_gbm_pt = [111, 112, 115]

        fermi_lat_pt = [120, 121, 127]

        super().__init__("Fermi", fermi_gbm_pt + fermi_lat_pt)

    def subscribe(self):
        return [
            "gcn.classic.voevent.FERMI_GBM_ALERT",
            "gcn.classic.voevent.FERMI_GBM_FIN_POS",
            "gcn.classic.voevent.FERMI_GBM_FLT_POS",
            "gcn.classic.voevent.FERMI_GBM_GND_POS",
            "gcn.classic.voevent.FERMI_GBM_LC",
            "gcn.classic.voevent.FERMI_GBM_POS_TEST",
            "gcn.classic.voevent.FERMI_GBM_SUBTHRESH",
            "gcn.classic.voevent.FERMI_GBM_TRANS",
            "gcn.classic.voevent.FERMI_LAT_GND",
            "gcn.classic.voevent.FERMI_LAT_MONITOR",
            "gcn.classic.voevent.FERMI_LAT_OFFLINE",
            "gcn.classic.voevent.FERMI_LAT_POS_DIAG",
            "gcn.classic.voevent.FERMI_LAT_POS_INI",
            "gcn.classic.voevent.FERMI_LAT_POS_TEST",
            "gcn.classic.voevent.FERMI_LAT_POS_UPD",
            "gcn.classic.voevent.FERMI_LAT_TRANS",
            "gcn.classic.voevent.FERMI_POINTDIR",
            "gcn.classic.voevent.FERMI_SC_SLEW",
        ]


class Swift(Instrument):
    def __init__(self):
        super().__init__("SWIFT", [97, 61, 65, 67, 81])

    def subscribe(self):
        return [
            "gcn.classic.voevent.SWIFT_ACTUAL_POINTDIR",
            "gcn.classic.voevent.SWIFT_BAT_ALARM_LONG",
            "gcn.classic.voevent.SWIFT_BAT_ALARM_SHORT",
            "gcn.classic.voevent.SWIFT_BAT_GRB_ALERT",
            "gcn.classic.voevent.SWIFT_BAT_GRB_LC",
            "gcn.classic.voevent.SWIFT_BAT_GRB_LC_PROC",
            "gcn.classic.voevent.SWIFT_BAT_GRB_POS_ACK",
            "gcn.classic.voevent.SWIFT_BAT_GRB_POS_NACK",
            "gcn.classic.voevent.SWIFT_BAT_GRB_POS_TEST",
            "gcn.classic.voevent.SWIFT_BAT_KNOWN_SRC",
            "gcn.classic.voevent.SWIFT_BAT_MONITOR",
            "gcn.classic.voevent.SWIFT_BAT_QL_POS",
            "gcn.classic.voevent.SWIFT_BAT_SCALEDMAP",
            "gcn.classic.voevent.SWIFT_BAT_SLEW_POS",
            "gcn.classic.voevent.SWIFT_BAT_SUB_THRESHOLD",
            "gcn.classic.voevent.SWIFT_BAT_SUBSUB",
            "gcn.classic.voevent.SWIFT_BAT_TRANS",
            "gcn.classic.voevent.SWIFT_FOM_OBS",
            "gcn.classic.voevent.SWIFT_FOM_PPT_ARG_ERR",
            "gcn.classic.voevent.SWIFT_FOM_SAFE_POINT",
            "gcn.classic.voevent.SWIFT_FOM_SLEW_ABORT",
            "gcn.classic.voevent.SWIFT_POINTDIR",
            "gcn.classic.voevent.SWIFT_SC_SLEW",
            "gcn.classic.voevent.SWIFT_TOO_FOM",
            "gcn.classic.voevent.SWIFT_TOO_SC_SLEW",
            "gcn.classic.voevent.SWIFT_UVOT_DBURST",
            "gcn.classic.voevent.SWIFT_UVOT_DBURST_PROC",
            "gcn.classic.voevent.SWIFT_UVOT_EMERGENCY",
            "gcn.classic.voevent.SWIFT_UVOT_FCHART",
            "gcn.classic.voevent.SWIFT_UVOT_FCHART_PROC",
            "gcn.classic.voevent.SWIFT_UVOT_POS",
            "gcn.classic.voevent.SWIFT_UVOT_POS_NACK",
            "gcn.classic.voevent.SWIFT_XRT_CENTROID",
            "gcn.classic.voevent.SWIFT_XRT_EMERGENCY",
            "gcn.classic.voevent.SWIFT_XRT_IMAGE",
            "gcn.classic.voevent.SWIFT_XRT_IMAGE_PROC",
            "gcn.classic.voevent.SWIFT_XRT_LC",
            "gcn.classic.voevent.SWIFT_XRT_POSITION",
            "gcn.classic.voevent.SWIFT_XRT_SPECTRUM",
            "gcn.classic.voevent.SWIFT_XRT_SPECTRUM_PROC",
            "gcn.classic.voevent.SWIFT_XRT_SPER",
            "gcn.classic.voevent.SWIFT_XRT_SPER_PROC",
            "gcn.classic.voevent.SWIFT_XRT_THRESHPIX",
            "gcn.classic.voevent.SWIFT_XRT_THRESHPIX_PROC",
        ]


class Integral(Instrument):
    def __init__(self):
        super().__init__("INTEGRAL", [53, 54, 56])

    def subscribe(self):
        return [
            "gcn.classic.voevent.INTEGRAL_OFFLINE",
            "gcn.classic.voevent.INTEGRAL_POINTDIR",
            "gcn.classic.voevent.INTEGRAL_REFINED",
            "gcn.classic.voevent.INTEGRAL_SPIACS",
            "gcn.classic.voevent.INTEGRAL_WAKEUP",
            "gcn.classic.voevent.INTEGRAL_WEAK",
        ]


class IceCube(Instrument):
    def __init__(self):
        super().__init__("ICECUBE", [157, 173, 174, 176])

    def subscribe(self):
        return [
            "gcn.classic.voevent.ICECUBE_ASTROTRACK_BRONZE",
            "gcn.classic.voevent.ICECUBE_ASTROTRACK_GOLD",
            "gcn.classic.voevent.ICECUBE_CASCADE",
        ]


FERMI = Fermi()
SWIFT = Swift()
INTEGRAL = Integral()
ICECUBE = IceCube()

LISTEN_PACKS = FERMI.packet_type + SWIFT.packet_type + INTEGRAL.packet_type + ICECUBE.packet_type
INSTR_SUBSCRIBES = FERMI.subscribe() + SWIFT.subscribe() + ICECUBE.subscribe() + INTEGRAL.subscribe()


def detect_instruments(gcn_description):
    if FERMI in str(gcn_description):
        return FERMI.__str__()
    elif SWIFT in str(gcn_description):
        return SWIFT.__str__()
    elif INTEGRAL in str(gcn_description):
        return INTEGRAL.__str__()
    elif ICECUBE in str(gcn_description):
        return ICECUBE.__str__()
    else:
        raise ValueError("Unknown instruments in the system: {}".format(gcn_description))


