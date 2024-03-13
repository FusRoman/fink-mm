import voeventparse as vp
import os.path as path

from fink_mm.observatory import OBSERVATORY_PATH
from fink_mm.observatory.observatory import Observatory, BadInstrument


class Swift(Observatory):
    """
    Swift instrument
    """

    def __init__(self, voevent):
        """
        Initialise a Swift class

        Parameters
        ----------
        voevent: ObjectifiedElement

        Example
        -------
        >>> voevent = load_voevent_from_path(swift_bat_voevent_path, logger)
        >>> obs = voevent_to_class(voevent)
        >>> type(obs)
        <class 'Swift.Swift'>
        """
        super().__init__(path.join(OBSERVATORY_PATH, "Swift", "swift.json"), voevent)

    def get_trigger_id(self):
        """
        Get the triggerId of the voevent

        Example
        -------
        >>> swift_bat.get_trigger_id()
        '1118357'
        >>> swift_xrt.get_trigger_id()
        '1120270'
        >>> swift_uvot.get_trigger_id()
        '1121751'
        """
        toplevel_params = vp.get_toplevel_params(self.voevent)

        return toplevel_params["TrigID"]["value"]

    def err_to_arcminute(self):
        """
        Return the error radius of the voevent in arcminute

        Example
        -------
        >>> swift_bat.err_to_arcminute()
        3.0
        >>> swift_xrt.err_to_arcminute()
        0.096
        >>> swift_uvot.err_to_arcminute()
        0.006
        """
        instrument = self.detect_instruments()
        coords = vp.get_event_position(self.voevent)

        err = 1 / 60 if coords.err == 0.0 else coords.err * 60

        if instrument in ["XRT", "UVOT", "BAT", "FOM"]:
            return err
        else:
            raise BadInstrument("{} is not a Swift instrument".format(instrument))
