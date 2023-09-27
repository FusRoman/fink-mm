import sys
import os
from enum import Flag, auto
from dateutil import parser

from astropy.time import Time

import fink_mm
import fink_mm.offline.spark_offline as offline
import fink_mm.online.ztf_join_gcn as online
import fink_mm.distribution.distribution as distrib


class Application(Flag):
    OFFLINE = auto()
    ONLINE = auto()
    DISTRIBUTION = auto()

    def build_application(self, logger, **kwargs):
        """
        Return the command line application

        Parameters
        ----------
        logger : logging object
            the logger used to print logs
        **kwargs : dictionary
            keywords arguments (application dependants)
            * OFFLINE:
                hbase_catalog, gcn_datapath_prefix, grb_datapath_prefix, night,
                    time_window, ast_dist, pansstar_dist, pansstar_star_score, gaia_dist
            * ONLINE:
                ztf_datapath_prefix, gcn_datapath_prefix, grb_datapath_prefix, night,
                    exit_after, tinterval, ast_dist, pansstar_dist, pansstar_star_score, gaia_dist
            * DISTRIBUTION:
                grbdata_path, night, tinterval, exit_after,
                    kafka_broker, username_writer, password_writer

        Returns
        -------
        application : String
            command line application to append to a spark-submit
        """
        if self == Application.OFFLINE:
            application = os.path.join(
                os.path.dirname(fink_mm.__file__),
                "offline",
                "spark_offline.py prod",
            )

            try:
                start_window_in_jd = (
                    Time(parser.parse(kwargs["night"]), format="datetime").jd + 0.49
                )  # + 0.49 to start the time window in the night

                application += " " + kwargs["hbase_catalog"]
                application += " " + kwargs["gcn_datapath_prefix"]
                application += " " + kwargs["grb_datapath_prefix"]
                application += " " + kwargs["night"]
                application += " " + kwargs["NSIDE"]
                application += " " + str(start_window_in_jd)
                application += " " + str(kwargs["time_window"])
                application += " " + kwargs["ast_dist"]
                application += " " + kwargs["pansstar_dist"]
                application += " " + kwargs["pansstar_star_score"]
                application += " " + kwargs["gaia_dist"]

                if kwargs["is_test"]:
                    application += " " + str(False)
                else:
                    application += " " + str(True)
            except Exception as e:
                logger.error("Parameter not found \n\t {}\n\t{}".format(e, kwargs))
                exit(1)

            return application
        elif self == Application.ONLINE:
            application = os.path.join(
                os.path.dirname(fink_mm.__file__),
                "online",
                "ztf_join_gcn.py prod",
            )

            try:
                application += " " + kwargs["ztf_datapath_prefix"]
                application += " " + kwargs["gcn_datapath_prefix"]
                application += " " + kwargs["grb_datapath_prefix"]
                application += " " + kwargs["night"]
                application += " " + kwargs["NSIDE"]
                application += " " + str(kwargs["exit_after"])
                application += " " + kwargs["tinterval"]
                application += " " + kwargs["ast_dist"]
                application += " " + kwargs["pansstar_dist"]
                application += " " + kwargs["pansstar_star_score"]
                application += " " + kwargs["gaia_dist"]
                application += " " + str(True) if kwargs["logs"] else " " + str(False)
                application += " " + kwargs["hdfs_adress"]
            except Exception as e:
                logger.error("Parameter not found \n\t {}\n\t{}".format(e, kwargs))
                exit(1)

            return application

        elif self == Application.DISTRIBUTION:
            application = os.path.join(
                os.path.dirname(fink_mm.__file__),
                "distribution",
                "distribution.py prod",
            )

            try:
                application += " " + kwargs["grb_datapath_prefix"]
                application += " " + kwargs["night"]
                application += " " + str(kwargs["exit_after"])
                application += " " + kwargs["tinterval"]
                application += " " + kwargs["kafka_broker"]
                application += " " + kwargs["username_writer"]
                application += " " + kwargs["password_writer"]
            except Exception as e:
                logger.error("Parameter not found \n\t {}\n\t{}".format(e, kwargs))
                exit(1)

            return application

    def run_application(self):
        """
        Run the application
        """
        if self == Application.OFFLINE:
            hbase_catalog = sys.argv[2]
            gcn_datapath_prefix = sys.argv[3]
            grb_datapath_prefix = sys.argv[4]
            night = sys.argv[5]
            NSIDE = int(sys.argv[6])
            start_window = float(sys.argv[7])
            time_window = int(sys.argv[8])

            ast_dist = float(sys.argv[9])
            pansstar_dist = float(sys.argv[10])
            pansstar_star_score = float(sys.argv[11])
            gaia_dist = float(sys.argv[12])

            column_filter = True if sys.argv[13] == "True" else False

            offline.spark_offline(
                hbase_catalog,
                gcn_datapath_prefix,
                grb_datapath_prefix,
                night,
                NSIDE,
                start_window,
                time_window,
                ast_dist,
                pansstar_dist,
                pansstar_star_score,
                gaia_dist,
                with_columns_filter=column_filter,
            )

        elif self == Application.ONLINE:
            ztf_datapath_prefix = sys.argv[2]
            gcn_datapath_prefix = sys.argv[3]
            grb_datapath_prefix = sys.argv[4]
            night = sys.argv[5]
            NSIDE = int(sys.argv[6])
            exit_after = sys.argv[7]
            tinterval = sys.argv[8]

            ast_dist = float(sys.argv[9])
            pansstar_dist = float(sys.argv[10])
            pansstar_star_score = float(sys.argv[11])
            gaia_dist = float(sys.argv[12])
            logs = True if sys.argv[13] == "True" else False
            hdfs_adress = sys.argv[14]

            online.ztf_join_gcn_stream(
                ztf_datapath_prefix,
                gcn_datapath_prefix,
                grb_datapath_prefix,
                night,
                NSIDE,
                exit_after,
                tinterval,
                hdfs_adress,
                ast_dist,
                pansstar_dist,
                pansstar_star_score,
                gaia_dist,
                logs,
            )

        elif self == Application.DISTRIBUTION:
            grbdata_path = sys.argv[2]
            night = sys.argv[3]
            exit_after = sys.argv[4]
            tinterval = sys.argv[5]
            kafka_broker = sys.argv[6]
            username_writer = sys.argv[7]
            password_writer = sys.argv[8]

            distrib.grb_distribution(
                grbdata_path,
                night,
                tinterval,
                exit_after,
                kafka_broker,
                username_writer,
                password_writer,
            )
