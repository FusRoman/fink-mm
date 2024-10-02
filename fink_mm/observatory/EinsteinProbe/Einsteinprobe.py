from fink_mm.observatory.observatory import Observatory
import os.path as path
from fink_mm.observatory import OBSERVATORY_PATH


class Einsteinprobe(Observatory):

    def __init__(self, notice: str):
        """
        Initialise a LVK class

        Parameters
        ----------
        voevent: ObjectifiedElement

        Example
        -------
        >>> ep_event = load_json_from_path(ep_initial_path, logger)
        >>> ep_alert = json_to_class(ep_event)
        >>> type(ep_alert)
        <class 'Einsteinprobe.Einsteinprobe'>
        """
        super().__init__(
            path.join(OBSERVATORY_PATH, "EinsteinProbe", "einsteinprobe.json"), notice
        )

    def is_observation(self) -> bool:
        """
        Test if the json is of type observation.

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
        >>> ep_alert.is_observation()
        True
        """
        return True

    def get_trigger_id(self):
        """
        Get the triggerId of the voevent

        Example
        -------
        >>> ep_alert.get_trigger_id()
        '01708973486'
        """
        return self.voevent["id"][0]

    def err_to_arcminute(self):
        """
        Return the error radius of the voevent in arcminute

        Example
        -------
        >>> ep_alert.err_to_arcminute()
        1.2
        """
        return self.voevent["ra_dec_error"] * 60
