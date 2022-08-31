import os
import configparser
from importlib.resources import files
import logging

from fink_grb import __name__


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

    output_path = config["PATH"]["online_gcn_data_prefix"]

    if not os.path.isdir(output_path):
        os.mkdir(output_path)
        os.mkdir(output_path + "/raw")


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

    """
    # create logger
    logger = logging.getLogger(__name__)
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
