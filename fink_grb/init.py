import os
import fink_grb
import configparser
from importlib.resources import files


def init_fink_grb(arguments):

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
        config_path = files('fink_grb').joinpath('conf/fink_grb.conf')
        config.read(config_path)

    output_path = config["PATH"]["gcn_path_storage"]

    if not os.path.isdir(output_path):
        os.mkdir(output_path)