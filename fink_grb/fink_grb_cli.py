"""
Usage:
    fink_grb start_gcn_stream [options]
    fink_grb launch_joining_stream --night=<date> --exit_after=<second> [options]
    fink_grb init [options]
    fink_grb -h | --help
    fink_grb --version

Options:
  init                             initialise the environment for fink_grb.
  start_gcn_stream                 start to listen the gcn stream.
  -h --help                        Show help and quit.
  --version                        Show version.
  --config FILE                    Specify the config file.
  --verbose                        Print information and progress bar during the process.
"""

from docopt import docopt
from fink_grb import __version__


def main():

    # parse the command line and return options provided by the user.
    arguments = docopt(__doc__, version=__version__)

    # The import are in the if statements to speed-up the cli execution.

    if arguments["start_gcn_stream"]:

        from fink_grb.online.gcn_stream import start_gcn_stream

        start_gcn_stream(arguments)

    elif arguments["init"]:

        from fink_grb.init import init_fink_grb

        init_fink_grb(arguments)

        exit(0)

    elif arguments["launch_joining_stream"]:

        from fink_grb.online.ztf_join_gcn import launch_joining_stream

        launch_joining_stream(arguments)

    else:
        exit(0)
