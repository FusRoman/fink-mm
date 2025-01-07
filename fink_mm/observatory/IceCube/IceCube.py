import pandas as pd
import datetime as dt
from astropy.time import Time
import voeventparse as vp
import os.path as path
from astropy.coordinates import SkyCoord
import astropy.units as u


from fink_mm.observatory import OBSERVATORY_PATH
from fink_mm.observatory.observatory import Observatory


class IceCube(Observatory):
    """
    IceCube instrument
    """

    def __init__(self, voevent):
        """
        Initialise a IceCube class

        Parameters
        ----------
        voevent: ObjectifiedElement

        Example
        -------
        >>> voevent = load_voevent_from_path(icecube_gold_voevent_path, logger)
        >>> obs = voevent_to_class(voevent)
        >>> type(obs)
        <class 'IceCube.IceCube'>
        """
        super().__init__(
            path.join(OBSERVATORY_PATH, "IceCube", "icecube.json"), voevent
        )

    def get_trigger_id(self):
        """
        Get the triggerId of the voevent

        Example
        -------
        >>> icecube_cascade.get_trigger_id()
        '13688925590129'
        >>> icecube_bronze.get_trigger_id()
        '13691845252263'
        >>> icecube_gold.get_trigger_id()
        '13746764735045'
        """
        toplevel_params = vp.get_toplevel_params(self.voevent)

        return toplevel_params["AMON_ID"]["value"]

    def detect_instruments(self):
        return self.voevent.attrib["ivorn"].split("#")[1].split("_")[1]

    def err_to_arcminute(self):
        """
        Return the error radius of the voevent in arcminute.

        Example
        -------
        >>> icecube_cascade.err_to_arcminute()
        773.778
        >>> icecube_bronze.err_to_arcminute()
        39.126
        >>> icecube_gold.err_to_arcminute()
        39.576
        """
        coords = vp.get_event_position(self.voevent)
        return coords.err * 60

    def voevent_to_df(self):
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
        >>> icecube_cascade.voevent_to_df()[["triggerId", "observatory", "instrument", "event", "ra", "dec", "err_arcmin"]]
                triggerId observatory instrument    event        ra     dec  err_arcmin
        0  13688925590129     ICECUBE             Cascade  150.1126 -9.1693     773.778

        >>> icecube_bronze.voevent_to_df()[["triggerId", "observatory", "instrument", "event", "ra", "dec", "err_arcmin"]]
                triggerId observatory instrument   event        ra      dec  err_arcmin
        0  13691845252263     ICECUBE             BRONZE  132.3328 -42.7168      39.126

        >>> icecube_gold.voevent_to_df()[["triggerId", "observatory", "instrument", "event", "ra", "dec", "err_arcmin"]]
                triggerId observatory instrument event        ra     dec  err_arcmin
        0  13746764735045     ICECUBE             GOLD  350.5486  34.711      39.576
        """

        ack_time = dt.datetime.now()
        trigger_id = self.get_trigger_id()

        coords = vp.get_event_position(self.voevent)
        time_utc = vp.get_event_time_as_utc(self.voevent)

        time_jd = Time(time_utc, format="datetime").jd

        voevent_error = self.err_to_arcminute()
        if voevent_error == 0:
            voevent_error = 1 / 60

        df = pd.DataFrame.from_dict(
            {
                "observatory": [self.observatory],
                "instrument": [""],
                "event": [self.detect_instruments()],
                "ivorn": [self.voevent.attrib["ivorn"]],
                "triggerId": [trigger_id],
                "ra": [coords.ra],
                "dec": [coords.dec],
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

    def association_proba(
        self, ztf_ra: float, ztf_dec: float, jdstarthist: float, **kwargs
    ) -> float:
        """
        Compute the association probability between the IceCube event and a ztf alerts

        Currently, test only if the ztf alerts fall within the error box of the event.
        Return -1.0 if not and return 0.5 otherwise.
        TODO: compute a better probability function between neutrino event and ztf alerts

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

        >>> icecube_bronze.association_proba(0, 0, 0)
        -1.0

        >>> tr_time = Time("2022-08-08T07:59:57.26").jd
        >>> icecube_bronze.association_proba(132.3328, -42.7168, tr_time)
        0.5
        """

        # array of error box
        event_error = self.err_to_arcminute()

        _, trigger_time_jd = self.get_trigger_time()

        # alerts emits after the grb
        delay = jdstarthist - trigger_time_jd
        time_condition = delay >= 0

        ztf_coords = SkyCoord(ztf_ra, ztf_dec, unit=u.degree)

        coords = vp.get_event_position(self.voevent)
        event_coord = SkyCoord(coords.ra, coords.dec, unit=u.degree)

        # alerts falling within the event_error_box
        spatial_condition = (
            ztf_coords.separation(event_coord).arcminute <= 1.5 * event_error
        )

        if time_condition and spatial_condition:
            return 0.5
        else:
            return -1.0
