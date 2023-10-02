import sys
import os
from enum import Flag, auto

import fink_mm
import fink_mm.online.ztf_join_gcn as online
import fink_mm.distribution.distribution as distrib
from fink_mm.utils.fun_utils import DataMode
from fink_mm.init import LoggerNewLine


class Application(Flag):
    JOIN = auto()
    DISTRIBUTION = auto()

    def build_application(self, data_mode: DataMode, logger: LoggerNewLine, **kwargs) -> str:
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
        if self == Application.JOIN:
            application = os.path.join(
                os.path.dirname(fink_mm.__file__),
                "online",
                "ztf_join_gcn.py",
            )

            if data_mode == DataMode.OFFLINE:
                application += " offline"
            elif data_mode == DataMode.STREAMING:
                application += " streaming"
            else:
                raise Exception(f"data_mode not exist: {data_mode}")

            try:
                application += " " + kwargs["ztf_datapath_prefix"]
                application += " " + kwargs["gcn_datapath_prefix"]
                application += " " + kwargs["grb_datapath_prefix"]
                application += " " + kwargs["night"]
                application += " " + kwargs["NSIDE"]
                application += " " + str(kwargs["exit_after"])
                application += " " + kwargs["tinterval"]
                application += " " + str(kwargs["time_window"])
                application += " " + kwargs["ast_dist"]
                application += " " + kwargs["pansstar_dist"]
                application += " " + kwargs["pansstar_star_score"]
                application += " " + kwargs["gaia_dist"]
                application += " " + str(True) if kwargs["logs"] else " " + str(False)
                application += " " + kwargs["hdfs_adress"]

                if kwargs["is_test"]:
                    application += " " + str(True)
                else:
                    application += " " + str(False)

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

    def run_application(self, data_mode: DataMode):
        """
        Run the application
        """
        if self == Application.JOIN:
            ztf_datapath_prefix = sys.argv[2]
            gcn_datapath_prefix = sys.argv[3]
            grb_datapath_prefix = sys.argv[4]
            night = sys.argv[5]
            NSIDE = int(sys.argv[6])
            exit_after = sys.argv[7]
            tinterval = sys.argv[8]
            time_window = sys.argv[9]
            ast_dist = float(sys.argv[10])
            pansstar_dist = float(sys.argv[11])
            pansstar_star_score = float(sys.argv[12])
            gaia_dist = float(sys.argv[13])
            logs = True if sys.argv[14] == "True" else False
            hdfs_adress = sys.argv[15]
            is_test = True if sys.argv[16] == "True" else False

            online.ztf_join_gcn(
                data_mode,
                ztf_datapath_prefix,
                gcn_datapath_prefix,
                grb_datapath_prefix,
                night,
                NSIDE,
                exit_after,
                tinterval,
                time_window,
                hdfs_adress,
                ast_dist,
                pansstar_dist,
                pansstar_star_score,
                gaia_dist,
                logs,
                is_test,
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
