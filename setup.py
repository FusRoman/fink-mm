#!/usr/bin/env python

from glob import glob
import os.path as path
from setuptools import setup, find_packages
import fink_mm

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name="fink-mm",
    version=fink_mm.__version__,
    description="Correlation of Fink alerts with notices from the General Coordinate Network (GCN)",
    author="Roman Le Montagner",
    author_email="roman.le-montagner@ijclab.in2p3.fr",
    url="https://github.com/FusRoman/Fink_MM",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    package_data={
        "fink_mm": [
            "conf/fink_mm.conf",
            "conf/fink_mm_schema_version_{}.avsc".format(
                fink_mm.__distribution_schema_version__
            ),
            "observatory/observatory_schema_version_{}.json".format(
                fink_mm.__observatory_schema_version__
            ),
        ]
        + [
            path.relpath(el, start="fink_mm")
            for el in glob("fink_mm/observatory/*/*.json")
        ]
    },
    install_requires=[
        "fink-utils>=0.8.0",
        "fink-client>=4.4",
        "fink-filters>=3.8",
        "docopt>=0.6.2",
        "terminaltables>=3.1.10",
        "numpy==1.21.6",
        "pandas==1.3.5",
        "astropy==4.3.1",
        "gcn-kafka>=0.1.2",
        "importlib-resources==5.9.0",
        "pyarrow==11.0.0",  # WARNING: Version upper than the fink-broker version (pyarrow==9.0.0)
        "pyspark==3.3.0",
        "scipy==1.7.3",
        "voevent-parse==1.0.3",
        "fastavro==1.6.0",
        "healpy==1.16.1",
        "tabulate==0.9.0",
        "jsonschema==4.6.0",
        "pytest==7.2.2",
        "pandera==0.14.5",
        "astropy_healpix==0.7",
    ],
    entry_points={"console_scripts": ["fink_mm=fink_mm.fink_mm_cli:main"]},
    license="Apache-2.0 License",
    platforms="Linux Debian distribution",
    project_urls={
        "Source": "https://github.com/FusRoman/Fink_MM",
    },
    python_requires=">=3.7",
)
