"""
Usage:
    fink_mm gcn_stream (start|monitor) [--restart] [options]
    fink_mm join_stream (offline|online) --night=<date> [--exit_after=<second>] [options]
    fink_mm distribute  --night=<date> [--exit_after=<second>] [options]
    fink_mm -h | --help
    fink_mm --version

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
from fink_mm import __version__


def main():
    # parse the command line and return options provided by the user.
    arguments = docopt(__doc__, version=__version__)

    # The import are in the if statements to speed-up the cli execution.

    if arguments["gcn_stream"]:
        if arguments["start"]:
            from fink_mm.gcn_stream.gcn_stream import start_gcn_stream

            start_gcn_stream(arguments)
        elif arguments["monitor"]:
            from fink_mm.utils.monitoring import gcn_stream_monitoring

            gcn_stream_monitoring(arguments)

    elif arguments["join_stream"]:
        if arguments["online"]:
            from fink_mm.online.ztf_join_gcn import launch_join
            from fink_mm.utils.application import DataMode

            launch_join(arguments, DataMode.STREAMING)

        elif arguments["offline"]:
            from fink_mm.online.ztf_join_gcn import launch_join
            from fink_mm.utils.application import DataMode

            launch_join(arguments, DataMode.OFFLINE)

    elif arguments["distribute"]:
        from fink_mm.distribution.distribution import launch_distribution

        launch_distribution(arguments)

    else:
        exit(0)
