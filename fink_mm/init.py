import os
import configparser
import datetime
import pytz

# from importlib.resources import files
from importlib_resources import files
import logging
import pathlib

import fink_mm


def return_verbose_level(config, logger):
    """
    Get the verbose level from the config file and return it.

    Parameters
    ----------
    config : dictionnary
        dictionnary containing the key values pair from the config file
    logger : logging object
        the logger used to print logs

    Returns
    -------
    logs : boolean
        if True, print the logs

    Examples
    --------
    >>> c = get_config({"--config" : "fink_mm/conf/fink_mm.conf"})
    >>> logger = init_logging()

    >>> return_verbose_level(c, logger)
    False
    """
    try:
        logs = config["ADMIN"]["verbose"] == "True"
    except Exception as e:
        logger.error(
            "Config entry not found \n\t {}\n\tsetting verbose to True by default".format(
                e
            )
        )
        logs = True

    return logs


def init_fink_mm(arguments):
    """
    Initialise the fink_mm environment. Get the config specify by the user with the
    --config argument or the default if not provided.

    Parameters
    ----------
    arguments : dictionnary
        arguments parse by docopt from the command line

    Returns
    -------
    None

    Examples
    --------
    >>> init_fink_mm({"--config" : None})
    >>> os.path.isdir("fink_mm/test/test_data/gcn_test/raw")
    True

    >>> os.path.isdir("fink_mm/test/test_output/grb")
    True
    """
    config = get_config(arguments)
    logger = init_logging()

    logs = return_verbose_level(config, logger)

    gcn_path = config["PATH"]["online_gcn_data_prefix"] + "/raw"
    grb_path = config["PATH"]["online_grb_data_prefix"] + "/grb"

    if not os.path.isdir(gcn_path):  # pragma: no cover
        pathlib.Path(gcn_path).mkdir(parents=True, exist_ok=True)
        if logs:
            logger.info("{} directory successfully created".format(gcn_path))

    if not os.path.isdir(grb_path):  # pragma: no cover
        pathlib.Path(grb_path).mkdir(parents=True, exist_ok=True)
        if logs:
            logger.info("{} directory successfully created".format(grb_path))


def get_config(arguments):
    """
    Get, read and return the configuration file of fink_mm

    Parameters
    ----------
    arguments : dictionnary
        arguments parse by docopt from the command line

    Returns
    -------
    config : ConfigParser
        the ConfigParser object containing the entry from the config file

    Examples
    --------
    >>> c = get_config({"--config" : "fink_mm/conf/fink_mm.conf"})
    >>> type(c)
    <class 'configparser.ConfigParser'>
    >>> c.sections()
    ['CLIENT', 'PATH', 'HDFS', 'PRIOR_FILTER', 'STREAM', 'DISTRIBUTION', 'ADMIN', 'OFFLINE']

    >>> c = get_config({"--config" : None})
    >>> type(c)
    <class 'configparser.ConfigParser'>
    >>> c.sections()
    ['CLIENT', 'PATH', 'HDFS', 'PRIOR_FILTER', 'STREAM', 'DISTRIBUTION', 'ADMIN', 'OFFLINE']
    """
    # read the config file
    config = configparser.ConfigParser(os.environ, interpolation=EnvInterpolation())

    if arguments["--config"]:
        if os.path.exists(arguments["--config"]):
            config.read(arguments["--config"])
        else:  # pragma: no cover
            print(
                "config file does not exist from this path: {} !!".format(
                    arguments["--config"]
                )
            )
            exit(1)
    else:
        config_path = files("fink_mm").joinpath("conf/fink_mm.conf")
        config.read(config_path)

    return config


class CustomTZFormatter(logging.Formatter):  # pragma: no cover
    """override logging.Formatter to use an aware datetime object"""

    def converter(self, timestamp):
        dt = datetime.datetime.fromtimestamp(timestamp)
        tzinfo = pytz.timezone("Europe/Paris")
        return tzinfo.localize(dt)

    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec="milliseconds")
            except TypeError:
                s = dt.isoformat()
        return s


class EnvInterpolation(configparser.BasicInterpolation):
    """Interpolation which expands environment variables in values."""

    def before_get(self, parser, section, option, value, defaults):
        value = super().before_get(parser, section, option, value, defaults)
        return os.path.expandvars(value)


def init_logging() -> logging.Logger:
    """
    Initialise a logger for the gcn stream

    Parameters
    ----------
    None

    Returns
    -------
    logger : Logger object
        A logger object for the logging management.

    Examples
    --------
    >>> l = init_logging()
    >>> type(l)
    <class 'logging.Logger'>
    """
    # create logger
    logger = logging.getLogger(fink_mm.__name__)
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = CustomTZFormatter(
        "%(asctime)s - %(name)s - %(levelname)s \n\t message: %(message)s"
    )

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

    return logger