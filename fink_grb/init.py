import os
import configparser

# from importlib.resources import files
from importlib_resources import files
import logging
import pathlib

import fink_grb
from fink_grb.grb_utils.fun_utils import return_verbose_level


def init_fink_grb(arguments):
    """
    Initialise the fink_grb environment. Get the config specify by the user with the
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
    >>> init_fink_grb({"--config" : None})
    >>> os.path.isdir("fink_grb/test/test_data/gcn_test/raw")
    True

    >>> os.path.isdir("fink_grb/test/test_output/grb")
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
    Get, read and return the configuration file of fink_grb

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
    >>> c = get_config({"--config" : "fink_grb/conf/fink_grb.conf"})
    >>> type(c)
    <class 'configparser.ConfigParser'>
    >>> c.sections()
    ['CLIENT', 'PATH', 'STREAM']

    >>> c = get_config({"--config" : None})
    >>> type(c)
    <class 'configparser.ConfigParser'>
    >>> c.sections()
    ['CLIENT', 'PATH', 'STREAM', 'ADMIN']
    """
    # read the config file
    config = configparser.ConfigParser(os.environ)

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
        config_path = files("fink_grb").joinpath("conf/fink_grb.conf")
        config.read(config_path)

    return config


def init_logging():
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
    logger = logging.getLogger(fink_grb.__name__)
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s \n\t message: %(message)s"
    )

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

    return logger


if __name__ == "__main__":  # pragma: no cover
    import sys
    import doctest
    from pandas.testing import assert_frame_equal  # noqa: F401
    import pandas as pd  # noqa: F401
    import shutil  # noqa: F401

    if "unittest.util" in __import__("sys").modules:
        # Show full diff in self.assertEqual.
        __import__("sys").modules["unittest.util"]._MAX_LENGTH = 999999999

    sys.exit(doctest.testmod()[0])
