class Instrument:
    """
    Main class for the instrument.
    """

    def __init__(self, instruments_name, packet_type):
        """
        Initialise an instrument.

        Parameters
        ----------
        instruments : string
            Name of the instrument
        packet_type : int list
            List of packet_type to listen.
        """
        self.instruments = instruments_name
        self.packet_type = packet_type

    def __str__(self) -> str:
        return self.instruments

    def __repr__(self) -> str:  # pragma: no cover
        return self.instruments


class Fermi(Instrument):
    """
    Fermi instrument
    """

    def __init__(self):

        fermi_gbm_pt = [111, 112, 115]

        fermi_lat_pt = [120, 121, 127]

        fermi_lat_transient_monitor = [123, 125]

        super().__init__("Fermi", fermi_gbm_pt + fermi_lat_pt + fermi_lat_transient_monitor)

    def subscribe(self):
        """
        Return the topics to listen for the Fermi instrument.
        """
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
    """
    Swift Instrument
    """

    def __init__(self):
        swift_transient_pt = [84]
        super().__init__("SWIFT", [61, 63, 65, 67, 81, 97] + swift_transient_pt)

    def subscribe(self):
        """
        Return the topics to listen for the Swift instrument.
        """
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
    """
    Integral instrument
    """

    def __init__(self):
        super().__init__("INTEGRAL", [53, 54, 56])

    def subscribe(self):
        """
        Return the topics to listen for the Integral instrument.
        """
        return [
            "gcn.classic.voevent.INTEGRAL_OFFLINE",
            "gcn.classic.voevent.INTEGRAL_POINTDIR",
            "gcn.classic.voevent.INTEGRAL_REFINED",
            "gcn.classic.voevent.INTEGRAL_SPIACS",
            "gcn.classic.voevent.INTEGRAL_WAKEUP",
            "gcn.classic.voevent.INTEGRAL_WEAK",
        ]


class IceCube(Instrument):
    """
    IceCube Instrument
    """

    def __init__(self):
        super().__init__("ICECUBE", [157, 173, 174, 176])

    def subscribe(self):
        """
        Return the topics to listen for the IceCube instrument.
        """
        return [
            "gcn.classic.voevent.ICECUBE_ASTROTRACK_BRONZE",
            "gcn.classic.voevent.ICECUBE_ASTROTRACK_GOLD",
            "gcn.classic.voevent.ICECUBE_CASCADE",
        ]


FERMI = Fermi()
SWIFT = Swift()
INTEGRAL = Integral()
ICECUBE = IceCube()

LISTEN_PACKS = (
    FERMI.packet_type + SWIFT.packet_type + INTEGRAL.packet_type + ICECUBE.packet_type
)
INSTR_SUBSCRIBES = (
    FERMI.subscribe() + SWIFT.subscribe() + ICECUBE.subscribe() + INTEGRAL.subscribe()
)

ALL_INSTRUMENTS = [FERMI, SWIFT, INTEGRAL, ICECUBE]


def detect_platform(gcn_description):
    """
    Detect the platform that emitted the voevent in the description field.

    Parameters
    ----------
    gcn_description : string
        Description field contains in the voevent.

    Returns
    -------
    instrument : string
        The emitting platform of the voevent.

    Examples
    --------

    >>> detect_platform('ivo://nasa.gsfc.gcn/SWIFT#Point_Dir_2022-08-31T23:32:00.00_53435214-375')
    'SWIFT'
    >>> detect_platform('ivo://nasa.gsfc.gcn/INTEGRAL#Point_Dir_2022-08-31T11:07:20.99_000000-179')
    'INTEGRAL'
    >>> detect_platform('ivo://nasa.gsfc.gcn/Fermi#Point_Dir_2022-08-30T23:16:00.00_000000-0-274')
    'Fermi'
    >>> detect_platform('ivo://nasa.gsfc.gcn/AMON#ICECUBE_BRONZE_Event2022-08-08T07:59:57.26_25_136918_045252263_0')
    'ICECUBE'
    """
    if FERMI.__str__() in str(gcn_description):
        return FERMI.__str__()
    elif SWIFT.__str__() in str(gcn_description):
        return SWIFT.__str__()
    elif INTEGRAL.__str__() in str(gcn_description):
        return INTEGRAL.__str__()
    elif ICECUBE.__str__() in str(gcn_description):
        return ICECUBE.__str__()
    else:  # pragma: no cover
        raise ValueError(
            "Unknown instruments in the system: {}".format(gcn_description)
        )


def detect_instruments(ivorn):
    """
    Detect the instrument that emitted the voevent in the ivorn field.

    Parameters
    ----------
    ivorn : string
        ivorn field contains in the voevent.

    Returns
    -------
    instrument : string
        The emitting instrument of the voevent.

    Examples
    --------

    >>> detect_instruments('ivo://nasa.gsfc.gcn/SWIFT#Point_Dir_2022-08-31T23:32:00.00_53435214-375')
    'Point'
    >>> detect_instruments('ivo://nasa.gsfc.gcn/INTEGRAL#Point_Dir_2022-08-31T11:07:20.99_000000-179')
    'Point'
    >>> detect_instruments('ivo://nasa.gsfc.gcn/Fermi#Point_Dir_2022-08-30T23:16:00.00_000000-0-274')
    'Point'
    >>> detect_instruments('ivo://nasa.gsfc.gcn/AMON#ICECUBE_BRONZE_Event2022-08-08T07:59:57.26_25_136918_045252263_0')
    'BRONZE'
    """
    split_ivorn = ivorn.split("#")[1].split("_")
    if split_ivorn[0] == "ICECUBE":
        return split_ivorn[1]
    else:
        return split_ivorn[0]


if __name__ == "__main__":  # pragma: no cover
    import sys
    import doctest

    if "unittest.util" in __import__("sys").modules:
        # Show full diff in self.assertEqual.
        __import__("sys").modules["unittest.util"]._MAX_LENGTH = 999999999

    sys.exit(doctest.testmod()[0])
