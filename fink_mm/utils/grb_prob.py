import numpy as np
from scipy.stats import poisson

# All constante necessary data for the grb proba

# From statistics computed on fink database
mean_ztf_transient_area = 31  # mean number of ztf transient per night per square degree
mean_ztf_transient_night = 141994  # mean number of ztf transient per night

mu_fermi = 250  # mean rate of grb detection by fermi (250 GRB/year)
mu_swift = 100  # mean rate of grb detection by swift (100 GRB/year)
mu_integral = 60  # mean rate of grb detection by integral (60 GRB/year)

ztf_cadence = 3750  # square degrees/hour (source: https://indico.cern.ch/event/848390/contributions/3614278/attachments/1964747/3266729/ZTF_LSST__KM3NET.pdf)

# we consider that ztf surveys the sky 4 hours per night
ztf_night_area = 3750 * 4
full_sky_area = (4 * np.pi) * (180 / np.pi) ** 2  # square degree
ztf_sky_fraction = ztf_night_area / full_sky_area

# mean rate of GRB detected for each instruments view by ZTF
mu_fermi_ztf = mu_fermi * ztf_sky_fraction
mu_swift_ztf = mu_swift * ztf_sky_fraction
mu_integral_ztf = mu_integral * ztf_sky_fraction


def mean_transient_in_grb_loc_area(
    grb_area: float,
) -> float:  # grb area -> radius error in degree
    """
    Compute the mean number of ZTF transient in a gamma ray burst (GRB) circle error area.

    Parameters
    ----------
    grb_area : float
        the GRB error radius in degree

    Returns
    -------
    float
        the mean number of transient that ZTF could detect in a GRB error circle.

    Examples
    --------

    >>> '%.6f' % mean_transient_in_grb_loc_area(1)
    '97.389372'

    >>> '%.6f' % mean_transient_in_grb_loc_area(10)
    '9738.937226'

    >>> '%.6f' % mean_transient_in_grb_loc_area(40)
    '155822.995618'

    >>> '%.6f' % mean_transient_in_grb_loc_area(38.635267)
    '145371.543736'
    """
    grb_area_square_degree = np.pi * grb_area**2
    return mean_ztf_transient_area * grb_area_square_degree  # number of transient


def proba_transient_in_grb_loc_area(grb_area: float) -> float:
    """
    Probability of finding a ZTF optical transient in the error circle of a GRB.

    Parameters
    ----------
    grb_area : float
        radius of the GRB circle error region in degree

    Returns
    -------
    float
        transient probability within the error region

    Examples
    --------
    >>> '%.6f' % proba_transient_in_grb_loc_area(0)
    '0.000000'

    >>> '%.6f' % proba_transient_in_grb_loc_area(1)
    '0.000686'

    >>> '%.6f' % proba_transient_in_grb_loc_area(10)
    '0.068587'

    >>> '%.6f' % proba_transient_in_grb_loc_area(40)
    '1.097391'

    >>> '%.6f' % proba_transient_in_grb_loc_area(38.635267)
    '1.023787'
    """
    return mean_transient_in_grb_loc_area(grb_area) / mean_ztf_transient_night


def lambda_poisson(time_interval: float, grb_rate: float) -> float:
    """
    Lambda Parameter of the Poisson probability distribution.
    The mean number of GRB during the time interval.

    Parameters
    ----------
    time_interval : float
        time interval in days between a ZTF optical transient and a GRB
    grb_rate : float
        the GRB detection rate of an instrument (mu_fermi_ztf, mu_swift_ztf or mu_integral_ztf)

    Returns
    -------
    float
        the mean number of GRB that could occurs during the time interval

    Examples
    --------
    >>> '%.6f' % lambda_poisson(1, mu_fermi_ztf)
    '0.248878'

    >>> '%.6f' % lambda_poisson(1, mu_swift_ztf)
    '0.099551'

    >>> '%.6f' % lambda_poisson(1, mu_integral_ztf)
    '0.059731'

    >>> '%.6f' % lambda_poisson(5, mu_fermi_ztf)
    '1.244388'

    >>> '%.6f' % lambda_poisson(5, mu_swift_ztf)
    '0.497755'

    >>> '%.6f' % lambda_poisson(5, mu_integral_ztf)
    '0.298653'

    >>> '%.6f' % lambda_poisson(10, mu_fermi_ztf)
    '2.488777'

    >>> '%.6f' % lambda_poisson(10, mu_swift_ztf)
    '0.995511'

    >>> '%.6f' % lambda_poisson(10, mu_integral_ztf)
    '0.597306'
    """
    # time_interval in days
    return grb_rate * (
        time_interval / 365.25
    )  # grb/year in the time interval given in days


def grb_proba_cdf(time_interval: float, grb_rate: float) -> float:
    """
    Compute the probability of having at least one grb in the given time delay

    Parameters
    ----------
    time_interval : float
        time interval in days between a ZTF optical transient and a GRB
    grb_rate : float
        the GRB detection rate of an instrument (mu_fermi_ztf, mu_swift_ztf or mu_integral_ztf)

    Returns
    -------
    float
        probability of having at least one grb in the given time delay

    Examples
    --------
    >>> '%.6f' % grb_proba_cdf(1, mu_fermi_ztf)
    '0.220325'

    >>> '%.6f' % grb_proba_cdf(1, mu_swift_ztf)
    '0.094756'

    >>> '%.6f' % grb_proba_cdf(1, mu_integral_ztf)
    '0.057982'

    >>> '%.6f' % grb_proba_cdf(5, mu_fermi_ztf)
    '0.711883'

    >>> '%.6f' % grb_proba_cdf(5, mu_swift_ztf)
    '0.392106'

    >>> '%.6f' % grb_proba_cdf(5, mu_integral_ztf)
    '0.258183'

    >>> '%.6f' % grb_proba_cdf(10, mu_fermi_ztf)
    '0.916989'

    >>> '%.6f' % grb_proba_cdf(10, mu_swift_ztf)
    '0.630465'

    >>> '%.6f' % grb_proba_cdf(10, mu_integral_ztf)
    '0.449708'
    """
    return 1 - poisson.cdf(
        0, lambda_poisson(time_interval, grb_rate)
    )  # probability of having at least one grb in the given delay


def serendipitous_association_proba(
    grb_rate: float, delay: float, grb_loc_area: float
) -> float:
    """
    Probability of having a GRB in the given time delay between the ZTF
    optical transient and having a ZTF optical transient in the GRB error circle.

    Parameters
    ----------
    grb_rate : float
        the GRB detection rate of an instrument (mu_fermi_ztf, mu_swift_ztf or mu_integral_ztf)
        unit: detection per year
    delay : float
        time interval in days between a ZTF optical transient and a GRB
        unit: day
    grb_loc_area : float
        radius of the GRB circle error region in degree
        unit: degree

    Returns
    -------
    float
        serendipitous probability of having a GRB in the delay and
        an optical transient in the circle error region.

    Examples
    --------
    ZTF21aagwbjr / GRB210204A
    >>> '%.6f' % serendipitous_association_proba(mu_fermi_ztf, 1.03, 3.1)
    '0.998510'

    ZTF21aakruew / GRB210212B
    >>> '%.6f' % serendipitous_association_proba(mu_fermi_ztf, 0.29, 36.327284855876584)
    '0.936975'

    ZTF23abaanxz / GRB230827B
    >>> '%.6f' % serendipitous_association_proba(mu_fermi_ztf, 0.083279, 2.17)
    '0.999934'
    """
    # proba of having a grb in a such delay between the optical alert and the trigger time
    # and having a ztf optical transient in the grb location area
    return 1 - proba_transient_in_grb_loc_area(grb_loc_area) * grb_proba_cdf(
        delay, grb_rate
    )
