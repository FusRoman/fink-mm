import json
import voeventparse as vp
import os.path as path

from lxml.objectify import ObjectifiedElement

from fink_grb.observatory import OBSERVATORY_PATH
from fink_grb.observatory.observatory import Observatory, BadInstrument

class Fermi(Observatory):
    """
    Fermi instrument
    """

    def __init__(self, voevent: ObjectifiedElement):
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
        super().__init__(path.join(OBSERVATORY_PATH, "Fermi", "fermi.json"), voevent)

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
        toplevel_params = vp.get_toplevel_params(self.voevent)

        return int(toplevel_params["TrigID"]["value"])

    def err_to_arcminute(self):
        """
        Return the error box of the voevent in arcminute
        
        Example 
        -------
        >>> fermi_gbm.err_to_arcminute()
        680.4
        >>> print(round(fermi_lat.err_to_arcminute(), 4))
        0.0167
        """
        instrument = self.detect_instruments()
        coords = vp.get_event_position(self.voevent)
        err = 1/60 if coords.err == 0.0 else coords.err

        if instrument == "GBM":
            return err * 60
        elif instrument == "LAT":
            return err
        else:
            raise BadInstrument("{} is not a Fermi instrument".format(instrument))

if __name__ == "__main__":

    fermi = Fermi()
    print(fermi)