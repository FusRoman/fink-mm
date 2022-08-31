import os
import configparser
from importlib.resources import files


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

    output_path = config["PATH"]["gcn_path_storage"]

    if not os.path.isdir(output_path):
        os.mkdir(output_path)


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
