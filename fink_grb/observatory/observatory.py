import json
from jsonschema import validate
from importlib_resources import files
import voeventparse as vp
import datetime as dt
from astropy.time import Time
import pandas as pd
from lxml.objectify import ObjectifiedElement
from pandera import DataFrameSchema, Column, Check, Index, check_output
import numpy as np
import healpy as hp
import pandas as pd
from fink_utils.science.utils import ra2phi, dec2theta

from fink_grb.observatory import OBSERVATORY_JSON_SCHEMA_PATH


class AbstractClassException(Exception):
    pass


class BadInstrument(Exception):
    pass


voevent_df_schema = DataFrameSchema(
    columns={
        "observatory": Column(
            dtype="str",
            checks=None,
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "instrument": Column(
            dtype="str",
            checks=None,
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "event": Column(
            dtype="str",
            checks=None,
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "ivorn": Column(
            dtype="str",
            checks=None,
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "triggerId": Column(
            dtype="int64",
            checks=None,
            nullable=False,
            unique=True,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "ra": Column(
            dtype="float64",
            checks=[
                Check.greater_than_or_equal_to(min_value=0),
                Check.less_than_or_equal_to(max_value=360),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "dec": Column(
            dtype="float64",
            checks=[
                Check.greater_than_or_equal_to(min_value=-90),
                Check.less_than_or_equal_to(max_value=90),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "err_arcmin": Column(
            dtype="float64",
            checks=None,
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "triggerTimejd": Column(
            dtype="float64",
            checks=[
                Check.greater_than_or_equal_to(min_value=0)
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "triggerTimeUTC": Column(
            dtype="datetime64[ns, UTC]",
            checks=None,
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "rawEvent": Column(
            dtype="str",
            checks=None,
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
    },
    checks=None,
    index=Index(
        dtype="int64",
        checks=None,
        nullable=False,
        coerce=False,
        name=None,
        description=None,
        title=None,
    ),
    dtype=None,
    coerce=True,
    strict=False,
    name=None,
    ordered=False,
    unique=None,
    report_duplicates="all",
    unique_column_names=False,
    title=None,
    description=None,
)


class Observatory:
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
        >>> voevent = load_voevent_from_path(fermi_gbm_voevent_path)
        >>> obs = voevent_to_class(voevent)
        >>> type(obs)
        <class 'Fermi.Fermi'>
        """

        instr_path = files("fink_grb").joinpath(instr_file)

        with open(instr_path, "r") as f:
            instr_data = json.loads(f.read())

        with open(OBSERVATORY_JSON_SCHEMA_PATH, "r") as f:
            schema = json.loads(f.read())

        validate(instance=instr_data, schema=schema)

        self.observatory = instr_data["name"]
        self.packet_type = instr_data["packet_type"]
        self.topics = instr_data["kafka_topics"]
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

    def get_trigger_id(self):
        raise AbstractClassException(
            """
            Call get_trigger_id from Observatory.
            Observatory is an abstract class, cannot be instanciate"""
        )

    def err_to_arcminute(self):
        raise AbstractClassException(
            """
            Call err_to_arcminute from Observatory.
            Observatory is an abstract class, cannot be instanciate"""
        )

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

        coords = vp.get_event_position(self.voevent)
        time_utc = vp.get_event_time_as_utc(self.voevent)

        time_jd = Time(time_utc, format="datetime").jd

        voevent_error = self.err_to_arcminute()

        df = pd.DataFrame(
            {
                "observatory": [self.observatory],
                "instrument": [self.detect_instruments()],
                "event": [""],
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

    def get_pixels(self, NSIDE: int)-> list:
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
        >>> icecube_cascade.get_pixels(32)

        """
        coords = vp.get_event_position(self.voevent)
        voevent_error = self.err_to_arcminute()

        theta, phi = dec2theta(coords.dec), ra2phi(coords.ra)
        vec = hp.ang2vec(theta, phi)
        ipix_disc = hp.query_disc(NSIDE, vec, radius=np.radians(voevent_error / 60), inclusive=True)
        return ipix_disc


# command to call to run the doctest :
# pytest --doctest-modules fink_grb/observatory/observatory.py -W ignore::DeprecationWarning
