import pandas as pd
import datetime as dt
from astropy.time import Time
import voeventparse as vp
import os.path as path
from pandera import check_output


from fink_grb.observatory import OBSERVATORY_PATH
from fink_grb.observatory.observatory import (
    Observatory,
    BadInstrument,
    voevent_df_schema,
)


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
        >>> voevent = load_voevent_from_path(icecube_gold_voevent_path)
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
        13688925590129
        >>> icecube_bronze.get_trigger_id()
        13691845252263
        >>> icecube_gold.get_trigger_id()
        13746764735045
        """
        toplevel_params = vp.get_toplevel_params(self.voevent)

        return int(toplevel_params["AMON_ID"]["value"])

    def detect_instruments(self):
        return self.voevent.attrib["ivorn"].split("#")[1].split("_")[1]

    def err_to_arcminute(self):
        """
        Return the error box of the voevent in arcminute

        Example
        -------
        >>> icecube_cascade.err_to_arcminute()
        773.778
        >>> icecube_bronze.err_to_arcminute()
        0.6521
        >>> icecube_gold.err_to_arcminute()
        0.6596
        """
        instrument = self.detect_instruments()
        coords = vp.get_event_position(self.voevent)

        if instrument == "BRONZE" or instrument == "GOLD":
            return coords.err
        elif instrument == "Cascade":
            return coords.err * 60
        else:
            raise BadInstrument("{} is not a IceCube events".format(instrument))

    @check_output(voevent_df_schema)
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
        0  13691845252263     ICECUBE             BRONZE  132.3328 -42.7168      0.6521

        >>> icecube_gold.voevent_to_df()[["triggerId", "observatory", "instrument", "event", "ra", "dec", "err_arcmin"]]
                triggerId observatory instrument event        ra     dec  err_arcmin
        0  13746764735045     ICECUBE             GOLD  350.5486  34.711      0.6596
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
                "rawEvent": vp.prettystr(self.voevent),
            }
        )

        df["year"] = df["triggerTimeUTC"].dt.strftime("%Y")
        df["month"] = df["triggerTimeUTC"].dt.strftime("%m")
        df["day"] = df["triggerTimeUTC"].dt.strftime("%d")

        return df
