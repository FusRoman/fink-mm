import os.path as path
from fink_grb.observatory import OBSERVATORY_PATH
from fink_grb.observatory.observatory import Observatory, BadInstrument


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
