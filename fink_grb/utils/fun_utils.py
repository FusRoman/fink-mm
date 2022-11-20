from pyarrow import fs


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
    >>> c = get_config({"--config" : "fink_grb/conf/fink_grb.conf"})
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


def get_hdfs_connector(host: str, port: int, user: str):
    """
    Initialise a connector to HDFS.

    To work, please read :
    https://arrow.apache.org/docs/python/filesystems.html#hadoop-distributed-file-system-hdfs

    The following command line can be used to locate the libhdfs.so file:
        `locate -l 1 libhdfs.so`

    Parameters
    ----------
    host: str
        IP address for the host machine
    port: int
        Port to access HDFS data.
    user: str
        Username on Hadoop.

    Returns
    -------
    fs: pyarrow.hdfs.HadoopFileSystem
    """
    return fs.HadoopFileSystem(host, port, user=user)  # work only for pyarrow >= 2.0.0


if __name__ == "__main__":  # pragma: no cover
    import sys
    import doctest
    from pandas.testing import assert_frame_equal  # noqa: F401
    import pandas as pd  # noqa: F401
    import shutil  # noqa: F401
    from fink_grb.init import get_config, init_logging  # noqa: F401

    if "unittest.util" in __import__("sys").modules:
        # Show full diff in self.assertEqual.
        __import__("sys").modules["unittest.util"]._MAX_LENGTH = 999999999

    sys.exit(doctest.testmod()[0])


def build_spark_submit(
    spark_submit, application, external_python_libs, spark_jars, packages
):
    """
    Build the spark submit command line to launch spark jobs.

    Parameters
    ----------
    spark_submit : string
        Initial spark_submit application containing the options the launch the jobs
    application : string
        The python script and their options that will be launched with the spark jobs
    external_python_libs : string
        list of external python module in .eggs format separated by ','.
    spark_jars : string
        list of external java libraries separated by ','.
    packages : string
        list of external java libraries hosted on maven, the java packages manager.

    Return
    ------
    spark_submit + application : string
        the initial spark_submit string with the additionnal options and libraries add to the spark_submit

    Examples
    --------
    >>> spark_submit = "spark-submit --master local[2] --driver-memory 8G --executor-memory 4G --conf spark.cores.max=4 --conf spark.executor.cores=2"
    >>> application = "myscript.py"
    >>> external_python_libs = "mypythonlibs.eggs,mypythonlibs2.py"
    >>> spark_jars = "myjavalib.jar,myjavalib2.jar"
    >>> packages = "org.apache.mylib:sublib:1.0.0"

    >>> build_spark_submit(spark_submit, application, external_python_libs, spark_jars, packages)
    'spark-submit --master local[2] --driver-memory 8G --executor-memory 4G --conf spark.cores.max=4 --conf spark.executor.cores=2 --py-files mypythonlibs.eggs,mypythonlibs2.py  --jars myjavalib.jar,myjavalib2.jar  --packages org.apache.mylib:sublib:1.0.0  myscript.py'

    >>> build_spark_submit(spark_submit, application, "", "", "")
    'spark-submit --master local[2] --driver-memory 8G --executor-memory 4G --conf spark.cores.max=4 --conf spark.executor.cores=2 myscript.py'
    """

    if application == "":
        raise ValueError("application parameters is empty !!")

    if external_python_libs != "":
        spark_submit += " --py-files {} ".format(external_python_libs)

    if spark_jars != "":
        spark_submit += " --jars {} ".format(spark_jars)

    if packages != "":
        spark_submit += " --packages {} ".format(packages)

    return spark_submit + " " + application
