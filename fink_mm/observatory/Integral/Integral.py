import voeventparse as vp
import os.path as path

from fink_mm.observatory import OBSERVATORY_PATH
from fink_mm.observatory.observatory import Observatory, BadInstrument


class Integral(Observatory):
    """
    Integral Observatory
    """

    def __init__(self, voevent):
        """
        Initialise a Integral class

        Parameters
        ----------
        voevent: ObjectifiedElement

        Example
        -------
        >>> voevent = load_voevent_from_path(integral_weak_voevent_path, logger)
        >>> obs = voevent_to_class(voevent)
        >>> type(obs)
        <class 'Integral.Integral'>
        """
        super().__init__(
            path.join(OBSERVATORY_PATH, "Integral", "integral.json"), voevent
        )

    def get_trigger_id(self):
        """
        Get the triggerId of the voevent

        Example
        -------
        >>> integral_weak.get_trigger_id()
        '9998'
        >>> integral_wakeup.get_trigger_id()
        '10042'
        >>> integral_refined.get_trigger_id()
        '75578'
        """
        toplevel_params = vp.get_toplevel_params(self.voevent)

        return toplevel_params["TrigID"]["value"]

    def err_to_arcminute(self):
        """
        Return the error radius of the voevent in arcminute.

        Example
        -------
        >>> integral_weak.err_to_arcminute()
        3.9360000000000004
        >>> integral_wakeup.err_to_arcminute()
        2.934
        >>> integral_refined.err_to_arcminute()
        2.586
        """
        instrument = self.detect_instruments()
        coords = vp.get_event_position(self.voevent)

        if instrument == "Weak" or instrument == "Wakeup" or instrument == "Refined":
            return coords.err * 60
        else:
            raise BadInstrument("{} is not a Integral events".format(instrument))
