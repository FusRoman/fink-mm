from pyspark import SparkConf
from pyspark.sql import SparkSession

import glob
import fastavro
import json
import io
import shutil
from fink_mm import __distribution_schema_version__

# This script is used to generate the avro schema used by the distribution
# The schema will be pushed into fink_mm/conf/ and its name will be 'fink_mm_schema_version_*SCHEMA_VERSION*.avro'
#   where *SCHEMA_VERSION* is the schema version

SCHEMA_VERSION = __distribution_schema_version__

def readschemadata(bytes_io: io._io.BytesIO) -> fastavro._read.reader:
    """Read data that already has an Avro schema.
    Parameters
    ----------
    bytes_io : `_io.BytesIO`
        Data to be decoded.
    Returns
    -------
    `fastavro._read.reader`
        Iterator over records (`dict`) in an avro file.
    Examples
    ----------
    Open an avro file, and read the schema and the records
    >>> with open(ztf_alert_sample, mode='rb') as file_data:
    ...   data = readschemadata(file_data)
    ...   # Read the schema
    ...   schema = data.schema
    ...   # data is an iterator
    ...   for record in data:
    ...     print(type(record))
    <class 'dict'>
    """
    bytes_io.seek(0)
    message = fastavro.reader(bytes_io)
    return message


def readschemafromavrofile(fn: str) -> dict:
    """Reach schema from a binary avro file.
    Parameters
    ----------
    fn: str
        Input Avro file with schema.
    Returns
    ----------
    schema: dict
        Dictionary (JSON) describing the schema.
    Examples
    ----------
    >>> schema = readschemafromavrofile(ztf_alert_sample)
    >>> print(schema['version'])
    3.3
    """
    with open(fn, mode="rb") as file_data:
        data = readschemadata(file_data)
        schema = data.schema
    return schema


package = "org.apache.spark:spark-avro_2.12:3.1.3"

conf = SparkConf()
confdic = {"spark.jars.packages": package}
conf.setMaster("local[2]")
conf.setAppName("fink_test")

for k, v in confdic.items():
    conf.set(key=k, value=v)

spark = SparkSession.builder.appName("fink_test").config(conf=conf).getOrCreate()

df = spark.read.format("parquet").load("fink_mm/test/test_data/online/")

df = df.drop("year").drop("month").drop("day").drop("timestamp").drop("t2")

df.printSchema()


path_for_avro = "fink_mm_schema_version_{}.avro".format(SCHEMA_VERSION)

df.coalesce(1).limit(1).write.format("avro").save(path_for_avro)

avro_file = glob.glob(path_for_avro + "/part*")[0]
avro_schema = readschemafromavrofile(avro_file)

# Write the schema to a file for decoding Kafka messages
with open("/tmp/{}".format(path_for_avro.replace(".avro", ".avsc")), "w") as f:
    json.dump(avro_schema, f, indent=2)

with open("/tmp/{}".format(path_for_avro.replace(".avro", ".avsc")), "r") as f:
    schema_ = json.dumps(f.read())

shutil.copy(
    "/tmp/{}".format(path_for_avro.replace(".avro", ".avsc")),
    "fink_mm/conf/{}".format(path_for_avro.replace(".avro", ".avsc")),
)

schema = json.loads(schema_)
print(schema)
