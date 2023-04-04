import pytest

from fink_grb.gcn_stream.gcn_reader import load_voevent
from fink_grb.observatory import voevent_to_class


@pytest.fixture(autouse=True)
def init_test(doctest_namespace):
    doctest_namespace["load_voevent"] = load_voevent
    doctest_namespace["voevent_to_class"] = voevent_to_class


@pytest.fixture(autouse=True)
def init_fermi(doctest_namespace):

    doctest_namespace["fermi_gbm_voevent_path"] = 'fink_grb/test/test_data/VODB/fermi/voevent_number=193.xml'
    doctest_namespace["fermi_lat_voevent_path"] = 'fink_grb/test/test_data/VODB/fermi/voevent_number=2842.xml'

    fermi_gbm = voevent_to_class(load_voevent(doctest_namespace["fermi_gbm_voevent_path"]))
    fermi_lat = voevent_to_class(load_voevent(doctest_namespace["fermi_lat_voevent_path"]))

    
    doctest_namespace["fermi_gbm"] = fermi_gbm
    doctest_namespace["fermi_lat"] = fermi_lat

@pytest.fixture(autouse=True)
def init_swift(doctest_namespace):

    doctest_namespace["swift_bat_voevent_path"] = 'fink_grb/test/test_data/VODB/swift/voevent_number=392.xml'
    doctest_namespace["swift_xrt_voevent_path"] = 'fink_grb/test/test_data/VODB/swift/voevent_number=4554.xml'
    doctest_namespace["swift_uvot_voevent_path"] = 'fink_grb/test/test_data/VODB/swift/voevent_number=8582.xml'

    swift_bat = voevent_to_class(load_voevent(doctest_namespace["swift_bat_voevent_path"]))
    swift_xrt = voevent_to_class(load_voevent(doctest_namespace["swift_xrt_voevent_path"]))
    swift_uvot = voevent_to_class(load_voevent(doctest_namespace["swift_uvot_voevent_path"]))

    doctest_namespace["swift_bat"] = swift_bat
    doctest_namespace["swift_xrt"] = swift_xrt
    doctest_namespace["swift_uvot"] = swift_uvot

@pytest.fixture(autouse=True)
def init_integral(doctest_namespace):

    doctest_namespace["integral_weak_voevent_path"] = 'fink_grb/test/test_data/VODB/integral/voevent_number=737.xml'
    doctest_namespace["integral_wakeup_voevent_path"] = 'fink_grb/test/test_data/VODB/integral/voevent_number=18790.xml'
    doctest_namespace["integral_refined_voevent_path"] = 'fink_grb/test/test_data/VODB/integral/voevent_number=18791.xml'

    integral_weak = voevent_to_class(load_voevent(doctest_namespace["integral_weak_voevent_path"]))
    integral_wakeup = voevent_to_class(load_voevent(doctest_namespace["integral_wakeup_voevent_path"]))
    integral_refined = voevent_to_class(load_voevent(doctest_namespace["integral_refined_voevent_path"]))

    doctest_namespace["integral_weak"] = integral_weak
    doctest_namespace["integral_wakeup"] = integral_wakeup
    doctest_namespace["integral_refined"] = integral_refined

@pytest.fixture(autouse=True)
def init_icecube(doctest_namespace):

    doctest_namespace["icecube_cascade_voevent_path"] = 'fink_grb/test/test_data/VODB/icecube/voevent_number=825.xml'
    doctest_namespace["icecube_bronze_voevent_path"] = 'fink_grb/test/test_data/VODB/icecube/voevent_number=3028.xml'
    doctest_namespace["icecube_gold_voevent_path"] = 'fink_grb/test/test_data/VODB/icecube/voevent_number=45412.xml'

    icecube_cascade = voevent_to_class(load_voevent(doctest_namespace["icecube_cascade_voevent_path"]))
    icecube_bronze = voevent_to_class(load_voevent(doctest_namespace["icecube_bronze_voevent_path"]))
    icecube_gold = voevent_to_class(load_voevent(doctest_namespace["icecube_gold_voevent_path"]))

    doctest_namespace["icecube_cascade"] = icecube_cascade
    doctest_namespace["icecube_bronze"] = icecube_bronze
    doctest_namespace["icecube_gold"] = icecube_gold