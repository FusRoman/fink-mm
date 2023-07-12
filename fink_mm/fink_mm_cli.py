"""
Usage:
    fink_grb gcn_stream (start|monitor) [--restart] [options]
    fink_grb join_stream (offline|online) --night=<date> [--exit_after=<second>] [options]
    fink_grb distribute  --night=<date> [--exit_after=<second>] [options]
    fink_grb -h | --help
    fink_grb --version

Options:
  gcn_stream                       used to manage the gcn stream.
  start                            start to listening the gcn stream
  monitor                          print informations about the status of the gcn stream process
                                   and the collected data.
  --restart                        restarts the gcn topics to the beginning.
  join_stream                      launch the script that join the ztf stream and the gcn stream
  offline                          launch the offline mode
  online                           launch the online mode
  distribute                       launch the distribution
  -h --help                        Show help and quit.
  --test                           launch the command in test mode.
  --version                        Show version.
  --config FILE                    Specify the config file.
  --verbose                        Print information during the process.
"""

from docopt import docopt
from fink_grb import __version__


def main():
    # parse the command line and return options provided by the user.
    arguments = docopt(__doc__, version=__version__)

    # The import are in the if statements to speed-up the cli execution.

    if arguments["gcn_stream"]:
        if arguments["start"]:
            from fink_grb.gcn_stream.gcn_stream import start_gcn_stream

            start_gcn_stream(arguments)
        elif arguments["monitor"]:
            from fink_grb.utils.monitoring import gcn_stream_monitoring

            gcn_stream_monitoring(arguments)

    elif arguments["join_stream"]:
        if arguments["online"]:
            from fink_grb.online.ztf_join_gcn import launch_joining_stream

            launch_joining_stream(arguments)

        elif arguments["offline"]:
            from fink_grb.offline.spark_offline import launch_offline_mode

            launch_offline_mode(arguments)

    elif arguments["distribute"]:
        from fink_grb.distribution.distribution import launch_distribution

        launch_distribution(arguments)

    else:
        exit(0)
