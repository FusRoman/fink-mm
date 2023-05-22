import os.path as path
import io
import numpy as np
import pandas as pd
import astropy.units as u
from astropy.time import Time
from astropy.table import Table
import astropy_healpix as ah
from base64 import b64decode
from pandera import check_output
import datetime as dt
import json

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
        Initialise a Fermi class

        Parameters
        ----------
        voevent: ObjectifiedElement

        Example
        -------
        >>> voevent = load_voevent_from_path(fermi_gbm_voevent_path)
        >>> obs = voevent_to_class(voevent)
        >>> type(obs)
        <class 'Fermi.Fermi'>
        """
        super().__init__(path.join(OBSERVATORY_PATH, "LVK", "lvk.json"), notice)

    def get_skymap(self) -> Table:
        """
        Decode and return the skymap

        Returns
        -------
        skymap: astropy.Table
            the sky localization error of the gw event as a skymap
        """
        skymap_str = self.voevent["event"]["skymap"]
        # Decode and parse skymap
        skymap_bytes = b64decode(skymap_str)
        skymap = Table.read(io.BytesIO(skymap_bytes))
        return skymap

    def is_observation(self, is_test: bool = False) -> bool:
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
        >>> fermi_gbm.is_observation()
        True
        """
        event_kind = "S" if not is_test else "M"
        return self.voevent["superevent_id"][0] != event_kind

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
        >>> fermi_gbm.detect_instruments()
        'GBM'
        """
        return "_".join(self.voevent["event"]["instruments"])

    def get_trigger_id(self):
        """
        Get the triggerId of the voevent

        Example
        -------
        >>> fermi_gbm.get_trigger_id()
        680782656
        >>> fermi_lat.get_trigger_id()
        1659883590
        """
        return self.voevent["superevent_id"]

    def get_trigger_time(self):
        time_utc = self.voevent["event"]["time"]
        time_jd = Time(time_utc, format="isot").jd
        return time_utc, time_jd

    def err_to_arcminute(self):
        """
        Return the 100% error area of the gw event in arcminute

        Example
        -------
        >>> fermi_gbm.err_to_arcminute()
        680.4
        >>> print(round(fermi_lat.err_to_arcminute(), 4))
        0.0167
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
        skymap = self.get_skymap()
        level, ipix = ah.uniq_to_level_ipix(
            skymap[np.argmax(skymap["PROBDENSITY"])]["UNIQ"]
        )
        ra, dec = ah.healpix_to_lonlat(ipix, ah.level_to_nside(level), order="nested")
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
        >>> fermi_gbm.voevent_to_df()[["triggerId", "observatory", "instrument", "event", "ra", "dec", "err_arcmin"]]
           triggerId observatory instrument event      ra     dec  err_arcmin
        0  680782656       Fermi        GBM        316.69 -4.1699       680.4
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
                "ra": [gw_ra.deg],
                "dec": [gw_dec.deg],
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

    def get_pixels(self, NSIDE: int) -> list:
        """
        Get the healpix pixels from the skymap

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
        array([10349])
        """
        skymap = self.get_skymap()
        level, ipix = ah.uniq_to_level_ipix(skymap["UNIQ"])
