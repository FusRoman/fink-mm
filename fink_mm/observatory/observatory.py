import json
from jsonschema import validate
from importlib_resources import files
import voeventparse as vp
import datetime as dt
from astropy.coordinates import SkyCoord
import pandas as pd
from lxml.objectify import ObjectifiedElement
import numpy as np
import healpy as hp
from abc import ABC, abstractmethod

import astropy.units as u
from astropy.time import Time

from fink_utils.science.utils import ra2phi, dec2theta

from fink_mm.observatory import OBSERVATORY_JSON_SCHEMA_PATH
from fink_mm.utils.grb_prob import serendipitous_association_proba


class BadInstrument(Exception):
    pass


class Observatory(ABC):
    """
    Main class for the instrument.
    """

    def __init__(self, instr_file: str, voevent: ObjectifiedElement):
        """
        Initialise an instrument.

        Parameters
        ----------
        instr_file : string
            Path of the .json describing an observatory
        voevent :
            List of packet_type to listen.

        Example
        -------
        >>> voevent = load_voevent_from_path(fermi_gbm_voevent_path, logger)
        >>> obs = voevent_to_class(voevent)
        >>> type(obs)
        <class 'Fermi.Fermi'>
        """

        instr_path = files("fink_mm").joinpath(instr_file)

        with open(instr_path, "r") as f:
            instr_data = json.loads(f.read())

        with open(OBSERVATORY_JSON_SCHEMA_PATH, "r") as f:
            schema = json.loads(f.read())

        validate(instance=instr_data, schema=schema)

        self.observatory = instr_data["name"]
        self.packet_type = instr_data["packet_type"]
        self.topics = instr_data["kafka_topics"]
        self.detection_rate = instr_data["grb_detection_rate"]
        self.voevent = voevent

    def __str__(self) -> str:
        return self.observatory

    def __repr__(self) -> str:
        return self.observatory

    def subscribe(self) -> list:
        return self.topics

    def is_observation(self) -> bool:
        """
        Test if the voevent is of type observation.

        Parameters
        ----------
        voevent : voevent object
            The voevent object.

        Returns
        -------
        is_observation : boolean
            Return True if the voevent is of observation type, otherwise return False

        Examples
        --------
        >>> fermi_gbm.is_observation()
        True
        """
        gcn_role = self.voevent.attrib["role"]
        return gcn_role == "observation"

    def is_listened_packets_types(self) -> bool:
        """
        Test if the voevent packet type correspond to those we listen to.

        Parameters
        ----------
        voevent : voevent object
            The voevent object.
        listen_pack : integer list
            The packet numbers type that we want to listen.

        Returns
        -------
        is_listen : boolean
            True if the voevent packet type is contained in listen pack, otherwise return False.

        Examples
        --------
        >>> fermi_gbm.is_listened_packets_types()
        True
        >>> fermi_gbm.packet_type = [0, 1, 2]
        >>> fermi_gbm.is_listened_packets_types()
        False
        """
        toplevel_params = vp.get_toplevel_params(self.voevent)
        gcn_packet_type = toplevel_params["Packet_Type"]["value"]

        return int(gcn_packet_type) in self.packet_type

    def detect_instruments(self) -> str:
        """
        Detect the instrument that emitted the voevent in the ivorn field.

        Parameters
        ----------

        Returns
        -------
        instrument : string
            The emitting instrument of the voevent.

        Examples
        --------
        >>> fermi_gbm.detect_instruments()
        'GBM'
        """

        return self.voevent.attrib["ivorn"].split("#")[1].split("_")[0]

    @abstractmethod
    def get_trigger_id(self):
        pass

    @abstractmethod
    def err_to_arcminute(self):
        pass

    def get_trigger_time(self):
        """
        Return the trigger time in UTC and julian date

        Returns
        -------
        time_utc: str
            utc trigger time
        time_jd: float
            julian date trigger time

        Example
        -------
        >>> icecube_gold.get_trigger_time()
        ('2022-12-23 07:43:00.520', 2459936.8215337964)
        """
        time_utc = vp.get_event_time_as_utc(self.voevent)
        time = Time(time_utc, format="datetime")

        return time.iso, time.jd

    def get_most_probable_position(self):
        """
        Return the equatorial coordinates of the most probable sky localization of this gw alert

        Returns
        -------
        ra: float
            right ascension
        dec: float
            declination

        Example
        -------
        >>> lvk_initial.get_most_probable_position()
        (95.712890625, -10.958863307027668)
        """
        coords = vp.get_event_position(self.voevent)
        return coords.ra, coords.dec

    def voevent_to_df(self) -> pd.DataFrame:
        """
        Convert a voevent object into a dataframe.

        Parameters
        ----------
        voevent : voevent object
            The voevent object.

        Returns
        -------
        df : dataframe
            A dataframe object containing some informations from the voevent.
            columns descriptions:
                - observatory: the observatory name (Example: IceCube, Swift, ...)
                - instruments : the instruments that send the voevent. (Example: GBM, XRT, ...)
                - event: the event that trigger the gcn. (Example: Cascade for IceCube, ...)
                - ivorn: the ivorn of the voevent. (Example: 'ivo://nasa.gsfc.gcn/AMON#ICECUBE_Cascade_Event2022-08-01T04:08:34.26_26_136889_025590129_0')
                - triggerId: the trigger_id of the voevent. (Example: 683499781)
                - ra: right ascension
                - dec: declination
                - err_arcmin: error box of the grb event (in arcminute)
                - triggerTimejd: trigger time of the voevent in julian date
                - triggerTimeUTC: trigger time of the voevent in UTC
                - rawEvent: the original voevent in xml format.

        Examples
        --------
        >>> fermi_gbm.voevent_to_df()[["triggerId", "observatory", "instrument", "event", "ra", "dec", "err_arcmin"]]
           triggerId observatory instrument event      ra     dec  err_arcmin
        0  680782656       Fermi        GBM        316.69 -4.1699       680.4
        """

        ack_time = dt.datetime.now()
        trigger_id = self.get_trigger_id()

        ra, dec = self.get_most_probable_position()

        time_utc, time_jd = self.get_trigger_time()

        voevent_error = self.err_to_arcminute()

        df = pd.DataFrame(
            {
                "observatory": [self.observatory],
                "instrument": [self.detect_instruments()],
                "event": [""],
                "ivorn": [self.voevent.attrib["ivorn"]],
                "triggerId": [trigger_id],
                "ra": [ra],
                "dec": [dec],
                "err_arcmin": [voevent_error],
                "ackTime": [ack_time],
                "triggerTimejd": [time_jd],
                "triggerTimeUTC": [time_utc],
                "raw_event": vp.prettystr(self.voevent),
            }
        )

        time_dt = Time(time_utc).to_datetime()

        df["year"] = time_dt.strftime("%Y")
        df["month"] = time_dt.strftime("%m")
        df["day"] = time_dt.strftime("%d")

        return df

    def get_pixels(self, NSIDE: int) -> list:
        """
        Compute the pixels within the error box of the voevent

        Parameters
        ----------
        NSIDE: integer
            Healpix map resolution, better if a power of 2

        Return
        ------
        ipix_disc: integer list
            all the pixels within the error area of the voevent

        Examples
        --------
        >>> icecube_bronze.get_pixels(32)
        array([10222, 10223, 10349])
        """
        coords = vp.get_event_position(self.voevent)
        voevent_error = self.err_to_arcminute()

        theta, phi = dec2theta(coords.dec), ra2phi(coords.ra)
        vec = hp.ang2vec(theta, phi)
        ipix_disc = hp.query_disc(
            NSIDE,
            vec,
            radius=np.radians(voevent_error / 60),
            inclusive=True,
            nest=False,
        )
        return ipix_disc

    def association_proba(
        self, ztf_ra: float, ztf_dec: float, jdstarthist: float, **kwargs
    ) -> float:
        """
        Compute the association probability between a gcn event and a ztf alerts

        This default function for the observatory class are reliable for GRB event.
        Overload this function to return a custom probability for other class of event.

        Parameters
        ----------
        ztf_ra : double spark column
            right ascension coordinates of the ztf alerts
        ztf_dec : double spark column
            declination coordinates of the ztf alerts
        jdstarthist : double spark column
            Earliest Julian date of epoch corresponding to ndethist [days]
            ndethist : Number of spatially-coincident detections falling within 1.5 arcsec
                going back to beginning of survey;
                only detections that fell on the same field and readout-channel ID
                where the input candidate was observed are counted.
                All raw detections down to a photometric S/N of ~ 3 are included.

        Return
        ------
        association_proba: double sp
            association probability
            0 <= proba <= 1 where closer to 0 implies higher likelihood of being associated with the events

        Examples
        --------
        >>> swift_bat.association_proba(0, 0, 0)
        -1.0

        >>> tr_time = Time("2022-07-30T15:48:54.89").jd
        >>> r = swift_bat.association_proba(225.0206, -69.4968, tr_time+1)
        >>> print(round(r, 13))
        0.999999998715

        >>> tr_time = Time("2022-07-29T21:13:01").jd
        >>> r = fermi_gbm.association_proba(316.6900, -4.1699, tr_time)
        >>> print(round(r, 6))
        0.999925

        >>> tr_time = Time("2022-09-26T10:38:37").jd
        >>> r = integral_refined.association_proba(273.9549, -37.2418, tr_time+10)
        >>> print(round(r, 12))
        0.999999994283
        """

        # grb detection rate (detection/year) for the self observatory
        grb_det_rate = self.detection_rate

        # array of error box
        grb_error = self.err_to_arcminute()

        _, trigger_time_jd = self.get_trigger_time()

        # alerts emits after the grb
        delay = jdstarthist - trigger_time_jd
        time_condition = delay > 0

        ztf_coords = SkyCoord(ztf_ra, ztf_dec, unit=u.degree)

        coords = vp.get_event_position(self.voevent)
        grb_coord = SkyCoord(coords.ra, coords.dec, unit=u.degree)

        # alerts falling within the grb_error_box
        spatial_condition = (
            ztf_coords.separation(grb_coord).arcminute <= 1.5 * grb_error
        )  # 63.5 * grb_error

        if time_condition and spatial_condition:
            # convert the delay in year
            delay_year = delay / 365.25

            # compute serendipitous probability
            return serendipitous_association_proba(
                grb_det_rate,
                delay_year,
                grb_error / 60
            )

        else:
            return -1.0


# command to call to run the doctest :
# pytest --doctest-modules fink_mm/observatory/observatory.py -W ignore::DeprecationWarning
