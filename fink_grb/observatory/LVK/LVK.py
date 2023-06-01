import os.path as path
import io
import numpy as np
import pandas as pd
import astropy.units as u
from astropy.time import Time
from astropy.table import QTable
import astropy_healpix as ah
from base64 import b64decode
from pandera import check_output
import datetime as dt
import json
from healpy.pixelfunc import pix2ang, ang2pix

from fink_grb.observatory import OBSERVATORY_PATH
from fink_grb.observatory.observatory import Observatory
from fink_grb.test.hypothesis.observatory_schema import voevent_df_schema


class LVK(Observatory):
    """
    LVK network
    Gravitational Wave Interferometers LIGO,Virgo,Kagra network
    """

    def __init__(self, notice: str):
        """
        Initialise a LVK class

        Parameters
        ----------
        voevent: ObjectifiedElement

        Example
        -------
        >>> lvk_event = load_json_from_path(lvk_initial_path, logger)
        >>> lvk_obs = json_to_class(lvk_event)
        >>> type(lvk_obs)
        <class 'LVK.LVK'>
        """
        super().__init__(path.join(OBSERVATORY_PATH, "LVK", "lvk.json"), notice)

    def get_skymap(self) -> QTable:
        """
        Decode and return the skymap

        Returns
        -------
        skymap: astropy.Table
            the sky localization error of the gw event as a skymap

        Examples
        --------
        >>> np.array(lvk_initial.get_skymap()["UNIQ"])
        array([  1285,   1287,   1296, ..., 162369, 162370, 162371])
        """
        skymap_str = self.voevent["event"]["skymap"]
        # Decode and parse skymap
        skymap_bytes = b64decode(skymap_str)
        skymap = QTable.read(io.BytesIO(skymap_bytes))
        return skymap

    def is_observation(self, is_test: bool) -> bool:
        """
        Test if the event is a real gw alert.

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
        >>> lvk_initial.is_observation(False)
        True
        >>> lvk_initial.is_observation(True)
        True
        >>> lvk_test.is_observation(True)
        True
        >>> lvk_test.is_observation(False)
        False
        """
        # Only respond to mock events. Real events have GraceDB IDs like
        # S1234567, mock events have GraceDB IDs like M1234567.
        # NOTE NOTE NOTE replace the conditional below with this commented out
        # conditional to only parse real events.
        if is_test:
            return (
                self.voevent["superevent_id"][0] == "S"
                or self.voevent["superevent_id"][0] == "M"
            )
        else:
            return self.voevent["superevent_id"][0] == "S"

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
        >>> lvk_initial.is_listened_packets_types()
        True
        >>> lvk_initial.packet_type = [0, 1, 2]
        >>> lvk_initial.is_listened_packets_types()
        False
        """
        return self.voevent["alert_type"] in self.packet_type

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
        >>> lvk_initial.detect_instruments()
        'H1_L1'
        """
        return "_".join(self.voevent["event"]["instruments"])

    def get_trigger_id(self):
        """
        Get the triggerId of the voevent

        Example
        -------
        >>> lvk_initial.get_trigger_id()
        'S230518h'
        """
        return self.voevent["superevent_id"]

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
        >>> lvk_initial.get_trigger_time()
        ('2023-05-18T12:59:08.167Z', 2460083.0410667476)
        """
        time_utc = self.voevent["event"]["time"]
        time_jd = Time(time_utc, format="isot").jd
        return time_utc, time_jd

    def err_to_arcminute(self):
        """
        Return the 100% error area of the gw event in arcminute

        Example
        -------
        >>> lvk_initial.err_to_arcminute()
        148510660.49790943
        """
        skymap = self.get_skymap()
        skymap.sort("PROBDENSITY", reverse=True)
        level, _ = ah.uniq_to_level_ipix(skymap["UNIQ"])
        pixel_area = ah.nside_to_pixel_area(ah.level_to_nside(level))

        prob = pixel_area * skymap["PROBDENSITY"]
        cumprob = np.cumsum(prob)

        i = cumprob.searchsorted(1)

        area = pixel_area[:i].sum()
        return area.to_value(u.arcmin**2)

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
        skymap = self.get_skymap()
        level, ipix = ah.uniq_to_level_ipix(
            skymap[np.argmax(skymap["PROBDENSITY"])]["UNIQ"]
        )
        lon, lat = ah.healpix_to_lonlat(ipix, ah.level_to_nside(level), order="nested")
        ra = lon.deg
        dec = lat.deg
        return ra, dec

    @check_output(voevent_df_schema)
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
        >>> lvk_initial.voevent_to_df()[["triggerId", "observatory", "instrument", "event", "ra", "dec", "err_arcmin"]]
          triggerId observatory instrument event         ra        dec    err_arcmin
        0  S230518h         LVK      H1_L1    gw  95.712891 -10.958863  1.485107e+08
        """

        ack_time = dt.datetime.now()
        trigger_id = self.get_trigger_id()

        gw_ra, gw_dec = self.get_most_probable_position()

        time_utc, time_jd = self.get_trigger_time()

        voevent_error = self.err_to_arcminute()

        df = pd.DataFrame(
            {
                "observatory": [self.observatory],
                "instrument": [self.detect_instruments()],
                "event": ["gw"],
                "ivorn": [""],
                "triggerId": [trigger_id],
                "ra": [gw_ra],
                "dec": [gw_dec],
                "err_arcmin": [voevent_error],
                "ackTime": [ack_time],
                "triggerTimejd": [time_jd],
                "triggerTimeUTC": pd.to_datetime(pd.Series([time_utc])),
                "raw_event": json.dumps(self.voevent),
            }
        )

        df["year"] = df["triggerTimeUTC"].dt.strftime("%Y")
        df["month"] = df["triggerTimeUTC"].dt.strftime("%m")
        df["day"] = df["triggerTimeUTC"].dt.strftime("%d")

        return df

    def find_probability_region(self, prob: float):
        """
        Return the region of a given probability

        Parameters
        ----------
        prob: float
            the probability of the region

        Return
        ------
        skymap_region_prob: Astropy Table
            the skymap containing the pixel within the probability region

        Example
        -------
        >>> map_70 = lvk_initial.find_probability_region(0.7)
        >>> len(map_70["UNIQ"])
        8074

        >>> map_90 = lvk_initial.find_probability_region(0.9)
        >>> len(map_90["UNIQ"])
        9704
        """
        skymap = self.get_skymap()
        skymap.sort("PROBDENSITY", reverse=True)
        level, _ = ah.uniq_to_level_ipix(skymap["UNIQ"])
        pixel_area = ah.nside_to_pixel_area(ah.level_to_nside(level))
        prob_area = pixel_area * skymap["PROBDENSITY"]
        cumprob = np.cumsum(prob_area)
        i = cumprob.searchsorted(prob)
        return skymap[:i]

    def get_pixels(self, NSIDE: int) -> np.ndarray:
        """
        Get the flat healpix pixels from a MOC skymap

        Parameters
        ----------
        NSIDE: integer
            Healpix flat map resolution, better if a power of 2

        Return
        ------
        ipix: integer list
            all the pixels within the 90% probability region of the skymap

        Examples
        --------
        >>> pix = lvk_initial.get_pixels(32)
        >>> pix
        array([  221,   254,   256, ..., 11348, 11418, 11419])
        >>> len(pix)
        1473
        """
        skymap_90 = self.find_probability_region(0.9)
        level, ipix = ah.uniq_to_level_ipix(skymap_90["UNIQ"])
        nside = ah.level_to_nside(level)
        theta, phi = pix2ang(nside, ipix)
        return np.unique(ang2pix(NSIDE, theta, phi))