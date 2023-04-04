import voeventparse as vp
from lxml.objectify import ObjectifiedElement


def load_voevent(file_path: str, verbose:bool = False) -> ObjectifiedElement:
    """
    Load a voevent from a file object.
    Raise an exception if the voevent cannot be parse.

    Parameters
    ----------
    file : string
        the path of the xml voevent file.
    verbose : boolean
        print additional information if an exception is raised.

    Returns
    -------
    voevent : voevent object
        The voevent object.

    Examples
    --------
    >>> f = open('fink_grb/test/test_data/voevent_number=9897.xml', 'rb')
    >>> v = load_voevent(f)
    >>> type(v)
    <class 'lxml.objectify.ObjectifiedElement'>
    >>> v.attrib['ivorn']
    'ivo://nasa.gsfc.gcn/Fermi#GBM_Fin_Pos2022-08-30T17:00:17.49_683571622_0-865'
    """
    try:
        with open(file_path, 'rb') as f:
            return vp.load(f)
    except Exception as e:  # pragma: no cover
        if verbose:
            print(
                "failed to load the voevent:\n\tlocation={}\n\tcause={}".format(file_path, e)
            )
        raise e


if __name__ == "__main__":  # pragma: no cover
    import sys
    import doctest
    from pandas.testing import assert_frame_equal  # noqa: F401
    import shutil  # noqa: F401
    import io  # noqa: F401

    if "unittest.util" in __import__("sys").modules:
        # Show full diff in self.assertEqual.
        __import__("sys").modules["unittest.util"]._MAX_LENGTH = 999999999

    sys.exit(doctest.testmod()[0])
