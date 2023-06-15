# Fink_GRB

[![PEP8](https://github.com/FusRoman/Fink_GRB/actions/workflows/linter.yml/badge.svg)](https://github.com/FusRoman/Fink_GRB/actions/workflows/linter.yml)
[![Sentinel](https://github.com/FusRoman/Fink_GRB/actions/workflows/run_test.yml/badge.svg)](https://github.com/FusRoman/Fink_GRB/actions/workflows/run_test.yml)

Correlation of the [Fink](https://fink-broker.org/) alerts with multi-messenger instruments for real-time purpose
* Available Instruments
    * Gamma-Ray: [Fermi](https://fermi.gsfc.nasa.gov/), [Swift](https://swift.gsfc.nasa.gov/about_swift/), [Integral](https://www.cosmos.esa.int/web/integral/home)
    * X-Ray: Swift
    * Optical: Swift
    * Neutrino: [IceCube](https://icecube.wisc.edu/)
    * Gravitational Waves: [LIGO](https://www.ligo.caltech.edu/), [VIRGO](https://www.virgo-gw.eu/), [KAGRA](https://gwcenter.icrr.u-tokyo.ac.jp/en/)

## Installation Procedure

* Install from GitHub with pip
```console
toto@linux:~$ pip install git+https://github.com/FusRoman/Fink_GRB@v0.6-beta
```
* Download the default config file from GitHub:fink_grb/conf/fink_grb.conf and move it in a custom location
Update your configuration file with your custom parameters.
Please note that the configuration file's paths must not end with a '/'.
* Once Fink_GRB has been installed via pip, the command `fink_grb` become available. Type `fink_grb --help` to see the help message and show what you can do with Fink_GRB.

### Setup the Fink_GRB daemons
* Start listening to GCN
```console
toto@linux:~$ fink_grb gcn_stream start --config /config_path 
```
The above command will start a daemon that will store the GCN issued from the instruments registered in the system. The GNC will be stored at the location specified in the configuration file by the entry named 'online_gcn_data_prefix'. The path can be a local path or a hdfs path. In the latter case, the path must start with hdfs://IP:PORT///your_path where IP and PORT refer to the hdfs driver.

> :warning: The GCN stream need to be restarted after each update of Fink_GRB. Use the `ps aux | grep fink_grb` command to identify the process number of the gcn stream and kill it.

### Schedulers
Fink_GRB has multiples script in the scheduler folder to launch the different services.
* science2grb.sh will launch the online services that will cross-match in real-time the alerts issued from the ZTF/LSST with incoming GCN. (latency: ZTF/LSST latency + 30 seconds max)
* science2grb_offline.sh launch the offline services that will cross-match all the latest alerts from ZTF/LSST with the GCN within the time window (in days) specified in your config file. (latency: 1 day)
* grb2distribution.sh launch the distribution services that will send the outputs of the online services in real-time to the registered users of the [fink-client](https://github.com/astrolabsoftware/fink-client). (latency: ZTF/LSST latency + 30 seconds + Network latency to reach fink-client)

#### **Modify the scripts**
Download the multiple scripts from GitHub:scheduler/
These scripts use some paths that have to be modified before the deployment.
* Two variables names `FINK_GRB_CONFIG` and `LOG_PATH` are common to all script. The first is the location of your configuration file, and the second is where to store log files. Either you modify the value of these variables directly in the scripts, science2grb.sh and science2grb_offline.sh or you remove the declaration in these scripts and export these variables within your ~/.bashrc or ~/.bash_profile.
```console
export FINK_GRB_CONFIG="path/to/conf/fink_grb.conf"
export LOG_PATH="path/to/store/log"
```

* For the online mode script, you must modify the variables `ZTF_ONLINE`, `GCN_ONLINE` and `ZTFXGRB_OUTPUT` with the same values as the following variables in the configuration file, respectively `online_ztf_data_prefix`, `online_gcn_data_prefix` and `online_grb_data_prefix`.

* For the distribution script, same as above, you must modify the variable name `ZTFXGRB_OUTPUT` with the same value as the field name `online_grb_data_prefix` in the configuration file.

* For the online and distribution script, change the value of the `HDFS_HOME` variable with the path of the HDFS installation.


#### **Cron jobs**
The three scripts are not meant to be launched lonely but with cron jobs. The following lines have to be put in the cron file.
```
0 01 * * * /custom_install_path/scheduler/science2grb.sh
0 01 * * * /custom_install_path/scheduler/grb2distribution.sh
1 17 * * * /custom_install_path/scheduler/science2grb_offline.sh
```
The above lines will launch the streaming services daily at 01:00 AM (Paris Timezone) until the end date specified in the scheduler script file. For both science2grb.sh and grb2distribution.sh, they finished at 05:00 PM (Paris Timezone). The start and end times have been set for ZTF (01:00 AM Paris -> 4:00 PM California / 5:00 PM Paris -> 08:00 AM California) and must be modified for LSST.
The offline services start at 5:01 PM daily and finish automatically at the end of the process. 

## Output description

The module output is pushed into the folder specified by the config entry named 'online_grb_data_prefix'.
The output could be local or in a HDFS cluster.
Two folders are created inside; one called 'online' and one called 'offline'. Inside these two folders, the data are repartitions following the same principle: 'year=year/month=month/day=day'. At the end of the path, you will find ```.parquet``` files containing the data.

### Fink_GRB Output Schema

|Field              |Type  |Contents                                                                          |
|-------------------|------|----------------------------------------------------------------------------------|
|Available for both online and offline mode                                                                   |
|ObjectId           |String|Unique ZTF identifier of the object                                               |
|candid             |int   |Unique ZTF identifier of the alert                                                |
|ztf_ra             |float |ZTF right ascension                                                               |
|ztf_dec            |float |ZTF declination                                                                   |
|fid                |int   |filter id (ZTF, g=1, r=2, i=3)                                                    |
|jdstarthist        |float |first variation time at 3 sigma of the object (in jd)                             |
|rb                 |float |real bogus score                                                                  |
|jd                 |float |alert emission time                                                               | 
|instrument_or_event|String|Triggering instruments (GBM, XRT, ...)                                            |
|platform           |String|Triggering platform (Fermi, Swift, IceCube, ...)                                  |
|triggerId          |String|Unique GCN identifier ([GCN viewer](https://heasarc.gsfc.nasa.gov/wsgi-scripts/tach/gcn_v2/tach.wsgi/) to retrieve quickly the notice)|
|grb_ra             |float |GCN Event right ascension                                                         |
|grb_dec            |float |GCN Event declination                                                             |
|grb_loc_error      |float |GCN error location in arcminute                                                   |
|triggerTimeUTC     |String|GCN TriggerTime in UTC                                                            |
|grb_proba          |float |Serendipitous probability to associate the alerts with the GCN event              |
|fink_class         |String|[Fink Classification](https://fink-broker.readthedocs.io/en/latest/science/classification/)                                                                                              |
|                                                                                                             |
|Field available only for the online mode                                                                     |
|delta_mag          |float |Difference of magnitude between the alert and the previous one                    |
|rate               |float |Magnitude rate (magnitude/day)                                                    |
|from_upper         |bool  |If true, the last alert used for the difference magnitude is an upper limit       |
|start_vartime      |float |first variation time at 5 sigma of the object (in jd, only valid for 30 days)     |
|diff_vartime       |float |difference between start_vartime and jd (if above 30, start_vartime is not valid) |

### Fink-client topics related to Fink_GRB

The connection to the distribution stream is made by the [fink-client](https://github.com/astrolabsoftware/fink-client). Three topics are available :

|Topic Name     | Description                                                                              |
|---------------|------------------------------------------------------------------------------------------|
|fink_grb_bronze| Alerts with a real bogus (rb) >= 0.5, classified by Fink as SN candidate, Unknown and Ambiguous within the error location of a GCN event.                                                                  |
|fink_grb_silver| Alerts satisfying the bronze filter with a grb_proba >= 5 sigma.                         |
|fink_grb_gold  |Alerts satisfying the silver filter with a mag_rate > 0.3 mag/day.                        |
