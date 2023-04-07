import voeventparse as vp
import io
from lxml.objectify import ObjectifiedElement


def load_voevent_from_path(file_path: str, verbose:bool = False) -> ObjectifiedElement:
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
    >>> v = load_voevent_from_path(fermi_gbm_voevent_path)
    >>> type(v)
    <class 'lxml.objectify.ObjectifiedElement'>
    >>> v.attrib['ivorn']
    'ivo://nasa.gsfc.gcn/Fermi#GBM_Fin_Pos2022-07-29T10:17:31.51_680782656_0-655'
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


def load_voevent_from_file(file: io.BufferedReader, verbose:bool = False) -> ObjectifiedElement:
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
    >>> f = open(fermi_gbm_voevent_path, 'rb')
    >>> v = load_voevent_from_file(f)
    >>> type(v)
    <class 'lxml.objectify.ObjectifiedElement'>
    >>> v.attrib['ivorn']
    'ivo://nasa.gsfc.gcn/Fermi#GBM_Fin_Pos2022-07-29T10:17:31.51_680782656_0-655'
    """
    try:
        return vp.load(file)
    except Exception as e:  # pragma: no cover
        if verbose:
            print(
                "failed to load the voevent:\n\tlocation={}\n\tcause={}".format(file, e)
            )
        raise e
