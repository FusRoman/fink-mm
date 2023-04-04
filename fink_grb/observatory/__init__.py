import fink_grb
import importlib.util
from importlib_resources import files
import sys
import os.path as path
from glob import glob
import gc

OBSERVATORY_PATH="observatory"
OBSERVATORY_JSON_SCHEMA_PATH = files("fink_grb").joinpath("observatory/observatory_schema_version_1.0.json")

all_instr = glob(
    path.join(
        path.dirname(fink_grb.__file__),
        "observatory/*/*.py"
    )
)
all_instr = [p for p in all_instr if path.basename(p) != "__init__.py"]

name_instr = [path.basename(el).split(".")[0].lower() for el in all_instr]

def __import_module(module_path):

    module_name = path.basename(module_path).split(".")[0]

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    foo = importlib.util.module_from_spec(spec)
    sys.modules[path.basename(module_name)] = foo
    spec.loader.exec_module(foo)

    return getattr(sys.modules[module_name], module_name)

__observatory_class = {n_instr: __import_module(path_instr) for n_instr, path_instr in zip(name_instr, all_instr)}

del name_instr
del all_instr
gc.collect()


def __get_detector(voevent):
    """
    Return the detector that emitted the voevent in the description field.

    Parameters
    ----------
    gcn_description : string
        Description field contains in the voevent.

    Returns
    -------
    instrument : string
        The emitting platform of the voevent.

    Examples
    --------

    >>> fermi_gbm_voevent = load_voevent(fermi_gbm_voevent_path)
    >>> __get_detector(fermi_gbm_voevent)
    'fermi'

    >>> swift_bat_voevent = load_voevent(swift_bat_voevent_path)
    >>> __get_detector(swift_bat_voevent)
    'swift'

    >>> icecube_gold_voevent = load_voevent(icecube_gold_voevent_path)
    >>> __get_detector(icecube_gold_voevent)
    'icecube'

    >>> integral_weak_voevent = load_voevent(integral_weak_voevent_path)
    >>> __get_detector(integral_weak_voevent)
    'integral'
    """
    ivorn = voevent.attrib["ivorn"]
    split_ivorn = ivorn.split("#")
    instr_name = path.basename(split_ivorn[0]).lower()

    if instr_name == "amon":
        return split_ivorn[1].split("_")[0].lower()

    return instr_name

# The fink_grb.observatory import have to be after the OBSERVATORY_JSON_SCHEMA_PATH definiton 
# to avoid a circular import issue
from fink_grb.observatory import observatory
from lxml.objectify import ObjectifiedElement
def voevent_to_class(voevent: ObjectifiedElement) -> observatory.Observatory:
    """
    Return the observatory class corresponding to the voevent

    Parameters
    ----------
    voevent: ObjectifiedElement
        a gcn voevent

    Return
    ------
    observatory: Observatory
        an observatory class

    Examples
    --------
    >>> fermi_gbm_voevent = load_voevent(fermi_gbm_voevent_path)
    >>> type(voevent_to_class(fermi_gbm_voevent))
    <class 'Fermi.Fermi'>

    >>> swift_bat_voevent = load_voevent(swift_bat_voevent_path)
    >>> type(voevent_to_class(swift_bat_voevent))
    <class 'Swift.Swift'>

    >>> icecube_gold_voevent = load_voevent(icecube_gold_voevent_path)
    >>> type(voevent_to_class(icecube_gold_voevent))
    <class 'IceCube.IceCube'>

    >>> integral_weak_voevent = load_voevent(integral_weak_voevent_path)
    >>> type(voevent_to_class(integral_weak_voevent))
    <class 'Integral.Integral'>
    """
    observatory_name = __get_detector(voevent)
    return __observatory_class[observatory_name.lower()](voevent)

