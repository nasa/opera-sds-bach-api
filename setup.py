from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from setuptools import setup, find_packages

setup(
    name="accountability_api",
    version="2.0.0",
    long_description="Data Accountability REST API using ElasticSearch backend",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "aniso8601>=8.0.0",
        "attrs>=21.2.0",
        "aws-requests-auth",
        "boto3>=1.24.1",
        "botocore",
        "certifi>=2021.5.30",
        "chardet>=4.0.0",
        "click>=8.0.0",
        "dicttoxml>=1.7.4",
        "elasticsearch>=7.13.4,<8.0.0",
        "Flask>=2.1.2",
        "Flask-Compress>=1.11",
        "Flask-Cors>=3.0.10",
        "flask-restx>=0.5.1",
        "idna>=3.2",
        "importlib-metadata>=4.11.4",
        "iniconfig>=1.1.1",
        "itsdangerous>=2.0.0",
        "Jinja2>=3.0.0",
        "jmespath>=1.0.0",
        "json2xml>=3.19.0",
        "jsonschema>=4.6.0",
        "lxml>=4.9.0",
        "MarkupSafe>=2.0.0",
        "matplotlib>=3.5.3",
        "more_itertools>=8.14.0",
        "numpy>=1.22.4",
        "packaging>=20.4",
        "pandas>=1.4.2",
        "pluggy>=0.13.1",
        "py>=1.9.0",
        "pyparsing>=2.4.7",
        "pyrsistent>=0.17.3",
        "python-dateutil>=2.8.1",
        "pytz>=2022.1",
        "requests>=2.26.0",
        "s3transfer>=0.3.0",
        "six>=1.15.0",
        "toml>=0.10.1",
        "urllib3>=1.26.7",
        "Werkzeug>=2.1.2",
        "xmltodict>=0.11.0",
        "zipp>=3.8.0",
    ],
    extras_require={
        'test': [
            "pytest>=7.1.3",
            "pytest-mock>=3.8.2",
            "coverage",
            "pytest-cov",
            "prov-es@https://github.com/hysds/prov_es/archive/refs/tags/v0.2.2.tar.gz",
            "osaka@https://github.com/hysds/osaka/archive/refs/tags/v1.1.0.tar.gz",
            "hysds-commons@https://github.com/hysds/hysds_commons/archive/refs/tags/v1.0.9.tar.gz",
            "hysds@https://github.com/hysds/hysds/archive/refs/tags/v1.1.5.tar.gz"
        ]
    }
)
